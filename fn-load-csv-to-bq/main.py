import functions_framework
from google.cloud import bigquery, storage
from datetime import datetime
import os
import re


def _to_bool(val, default=False):
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return val != 0
    if isinstance(val, str):
        return val.strip().lower() in {"true", "1", "t", "yes", "y", "si", "sí"}
    return default

def _sanitize_table_name(name: str) -> str:
    # BigQuery permite [A-Za-z0-9_], sin empezar por número para algunos casos de API; normalizamos.
    name = re.sub(r"[^A-Za-z0-9_]", "_", name)
    if name and name[0].isdigit():
        name = "_" + name
    return name

@functions_framework.http
def cargar_csv_a_bigquery(request):
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "Debe enviar un JSON con process_name, process_fn_name y arquetype_name"}, 400

        process_name = request_json.get("process_name")
        process_fn_name = request_json.get("process_fn_name")
        arquetype_name = request_json.get("arquetype_name")

        if not process_name or not process_fn_name or not arquetype_name:
            return {"error": "Faltan los campos 'process_name', 'process_fn_name' o 'arquetype_name'."}, 400

        bq_client = bigquery.Client()
        storage_client = storage.Client()

        # 1) Obtener parámetros desde BigQuery
        param_query = """
            SELECT params
            FROM `deinsoluciones-serverless.dev_config_zone.process_params`
            WHERE process_name = @process_name
              AND process_fn_name = @process_fn_name
              AND arquetype_name = @arquetype_name
              AND active = TRUE
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("process_fn_name", "STRING", process_fn_name),
                bigquery.ScalarQueryParameter("arquetype_name", "STRING", arquetype_name),
            ]
        )
        result = bq_client.query(param_query, job_config=job_config).result()
        row = next(iter(result), None)
        if not row:
            return {"error": "No se encontraron parámetros activos para la combinación solicitada."}, 404

        params_list = row["params"]
        if not params_list or not isinstance(params_list, list):
            return {"error": "Formato de 'params' inválido (se esperaba una lista con al menos un objeto)."}, 400

        params = params_list[0]

        input_uri    = params.get("input")              # gs://bucket/ruta/archivo.csv o prefijo
        output_table = params.get("output")             # project.dataset.table
        periodicidad = (params.get("periodicidad") or "").strip().lower()
        campo_fecha  = params.get("campo_fecha", "fecha_carga")

        csv_delimiter = params.get("csv_delimiter", ",")
        csv_header    = _to_bool(params.get("csv_header", True), default=True)

        if not input_uri or not output_table:
            return {"error": "Los campos 'input' y 'output' en params son obligatorios."}, 400
        if not input_uri.startswith("gs://"):
            return {"error": "El 'input' debe iniciar con 'gs://'."}, 400

        # 2) Parsear ruta GCS
        ruta_gcs = input_uri[5:]
        partes = ruta_gcs.split("/", 1)
        if len(partes) != 2:
            return {"error": "Formato inválido de 'input' (se esperaba gs://bucket/ruta/archivo.csv o prefijo)."}, 400

        bucket_name, path_completo = partes
        dir_path, file_name = os.path.split(path_completo)
        file_name_clean = os.path.basename(file_name) if file_name else ""
        nombre_base, _extension = os.path.splitext(file_name_clean)

        # 3) Resolver archivo según periodicidad
        resolved_input_uri = input_uri
        if periodicidad in {"mensual", "diario"}:
            prefix = dir_path + "/" if dir_path and not dir_path.endswith("/") else dir_path
            blobs = list(storage_client.list_blobs(bucket_name, prefix=prefix))
            candidatos = [
                blob for blob in blobs
                if blob.name.endswith(".csv") and (not nombre_base or nombre_base in os.path.basename(blob.name))
            ]
            if not candidatos:
                return {"error": f"No se encontró ningún archivo .csv que contenga '{nombre_base}' en gs://{bucket_name}/{prefix or ''}"}, 404
            archivo_mas_reciente = max(candidatos, key=lambda b: b.updated)
            resolved_input_uri = f"gs://{bucket_name}/{archivo_mas_reciente.name}"
            print(f"📂 Archivo más reciente detectado: {archivo_mas_reciente.name}")
        else:
            print(f"📂 Se utilizará el input proporcionado sin búsqueda adicional: {input_uri}")

        # 4) Definir IDs de dataset y tablas
        # output_table viene como project.dataset.table
        try:
            proj, dset, tbl = output_table.split(".")
        except ValueError:
            return {"error": "El 'output' debe tener el formato project.dataset.table"}, 400
        dataset_id = f"{proj}.{dset}"
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        staging_tbl_name = _sanitize_table_name(f"{tbl}_stg_{ts}")
        staging_table_id = f"{dataset_id}.{staging_tbl_name}"

        # 5) Cargar CSV a tabla staging con AUTODETECT
        staging_job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter=csv_delimiter,
            skip_leading_rows=1 if csv_header else 0,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,                     # ← detecta nombres y tipos
            allow_jagged_rows=True,
            encoding="UTF-8",
            quote='"',
            allow_quoted_newlines=True,
        )
        load_job = bq_client.load_table_from_uri(
            resolved_input_uri,
            staging_table_id,
            job_config=staging_job_config,
            location="us-east4",
        )
        load_job.result()
        staging_table = bq_client.get_table(staging_table_id)
        staging_schema = staging_table.schema

        # 6) Construir esquema destino: fechas se mantienen, el resto STRING
        dest_schema = []
        date_like = {"DATE", "DATETIME", "TIMESTAMP"}
        for f in staging_schema:
            ftype = f.field_type if f.field_type in date_like else "STRING"
            dest_schema.append(bigquery.SchemaField(f.name, ftype, mode=f.mode))

        # 7) Crear tabla destino si no existe
        dest_exists = True
        try:
            _ = bq_client.get_table(output_table)
        except Exception:
            dest_exists = False

        if not dest_exists:
            dest_tbl = bigquery.Table(output_table, schema=dest_schema)
            dest_tbl = bq_client.create_table(dest_tbl)
            print(f"🆕 Tabla destino creada: {output_table}")
        else:
            print(f"ℹ️ Tabla destino ya existe: {output_table}")

        # 8) Insertar datos: CAST a STRING excepto fechas/horas
        select_cols = []
        for f in staging_schema:
            if f.field_type in date_like:
                # Reafirmamos cast al mismo tipo para consistencia
                select_cols.append(f"CAST({bigquery._helpers._to_query_parameter(f.name)} AS {f.field_type}) AS `{f.name}`")
            else:
                select_cols.append(f"CAST({bigquery._helpers._to_query_parameter(f.name)} AS STRING) AS `{f.name}`")

        # Orden de columnas (coincidir con dest_schema)
        dest_col_names = [f.name for f in dest_schema]
        # Map rápido: nombre → expresión SELECT
        select_map = {re.search(r"AS\s+`(.+?)`$", expr).group(1): expr for expr in select_cols}
        ordered_select_exprs = [select_map[name] for name in dest_col_names if name in select_map]

        insert_sql = f"""
        INSERT INTO `{output_table}` ({", ".join(f"`{c}`" for c in dest_col_names)})
        SELECT {", ".join(ordered_select_exprs)}
        FROM `{staging_table_id}`
        """
        bq_client.query(insert_sql).result()

        # 9) Asegurar y actualizar campo_fecha
        if campo_fecha:
            alter_sql = f"""
            ALTER TABLE `{output_table}`
            ADD COLUMN IF NOT EXISTS `{campo_fecha}` TIMESTAMP
            """
            bq_client.query(alter_sql).result()

            update_sql = f"""
            UPDATE `{output_table}`
            SET `{campo_fecha}` = CAST(CURRENT_DATETIME("America/Santiago") AS TIMESTAMP)
            WHERE `{campo_fecha}` IS NULL
            """
            bq_client.query(update_sql).result()

        # 10) Limpieza: eliminar staging
        bq_client.delete_table(staging_table_id, not_found_ok=True)

        return {
            "message": f"CSV {resolved_input_uri} cargado en {output_table}. Fechas/datetimes preservados, restantes como STRING.",
            "csv_delimiter": csv_delimiter,
            "csv_header": bool(csv_header),
            "campo_fecha_actualizado": bool(campo_fecha),
            "status": "OK"
        }, 200

    except Exception as e:
        return {"error": str(e)}, 500
 