import express from "express";
import bodyParser from "body-parser";
import { DataformClient } from "@google-cloud/dataform";

const app = express();
app.use(bodyParser.json());

const client = new DataformClient();

// Utilidad para parsear string tipo "VAR1|VALOR1;VAR2|VALOR2"
function parseVarsString(varsStr) {
  const varsDict = {};
  const items = varsStr.split(";");
  for (const item of items) {
    const [key, value] = item.split("|");
    if (key && value) {
      varsDict[key.trim()] = value.trim();
    }
  }
  return varsDict;
}

app.post("/", async (req, res) => {
  const { repository, gitCommitish = "main", vars } = req.body;

  if (!repository || !vars) {
    return res.status(400).json({
      error: "Faltan parámetros requeridos: repository o vars"
    });
  }

  const projectId = "deinsoluciones-serverless";
  const region = "us-east4";

  const repoPath = `projects/${projectId}/locations/${region}/repositories/${repository}`;

  let parsedVars;

  // Detectar si vars viene como string plano o como objeto
  if (typeof vars === "string") {
    parsedVars = parseVarsString(vars);
  } else if (typeof vars === "object") {
    parsedVars = vars;
  } else {
    return res.status(400).json({
      error: "Formato de 'vars' no válido. Debe ser objeto o string tipo 'CLAVE|VALOR;...'"
    });
  }

  try {
    // 1. Crear CompilationResult
    const [compilationResult] = await client.createCompilationResult({
      parent: repoPath,
      compilationResult: {
        gitCommitish,
        codeCompilationConfig: {
          vars: parsedVars
        }
      }
    });

    // 2. Crear WorkflowInvocation usando compilationResult
    const [invocation] = await client.createWorkflowInvocation({
      parent: repoPath,
      workflowInvocation: {
        compilationResult: compilationResult.name,
        invocationConfig: {
          vars: parsedVars,
          serviceAccount: "77134593518-compute@developer.gserviceaccount.com"
        }
      }
    });

    res.status(200).json({
      message: "Ejecución iniciada correctamente",
      workflowInvocationName: invocation.name
    });

  } catch (error) {
    console.error("Error al iniciar ejecución de Dataform:", error);
    res.status(500).json({
      error: "No se pudo iniciar la ejecución",
      details: error.message
    });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`Servidor escuchando en el puerto ${PORT}`);
});
