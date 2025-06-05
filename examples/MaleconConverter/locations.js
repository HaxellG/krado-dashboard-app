// index.js
import { docClient } from './dynamoClient.js';
import { QueryCommand } from "@aws-sdk/lib-dynamodb";

const TABLE_NAME = process.env.TABLE_NAME || "locations";
const DEFAULT_LIMIT = 10; // Límite de ítems por página si no se especifica.

export const handler = async (event) => {
  // 1. --- Extraer Query Parameters ---
  const queryParams = event.queryStringParameters || {};
  const deviceId = queryParams.deviceId;
  const startTimestamp = queryParams.startTimestamp ? parseInt(queryParams.startTimestamp, 10) : null;
  const endTimestamp = queryParams.endTimestamp ? parseInt(queryParams.endTimestamp, 10) : null;
  const limit = queryParams.limit ? parseInt(queryParams.limit, 10) : DEFAULT_LIMIT;
  const page = queryParams.page ? parseInt(queryParams.page, 10) : 0;

  // Validar que `page` sea un entero >= 0
  if (isNaN(page) || page < 0) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: "El parámetro 'page' debe ser un número entero igual o mayor a 0." }),
    };
  }

  // 2. --- Validación de Parámetros ---
  if (!deviceId) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: "El parámetro 'deviceId' es requerido." }),
    };
  }
  if ((startTimestamp && !endTimestamp) || (!startTimestamp && endTimestamp)) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: "Debes proveer 'startTimestamp' y 'endTimestamp' juntos para el filtro de tiempo." }),
    };
  }
  if (startTimestamp && endTimestamp && startTimestamp >= endTimestamp) {
    return {
      statusCode: 400,
      body: JSON.stringify({ message: "'startTimestamp' debe ser menor que 'endTimestamp'." }),
    };
  }

  // 3. --- Construir bloque base de parámetros (sin ExclusiveStartKey) ---
  const baseParams = {
    TableName: TABLE_NAME,
    Limit: limit,
    KeyConditionExpression: 'deviceId = :deviceId',
    ExpressionAttributeValues: {
      ':deviceId': deviceId,
    },
    ScanIndexForward: true, // true = ascendente (más viejo a más nuevo)
  };

  if (startTimestamp && endTimestamp) {
    baseParams.KeyConditionExpression += ' AND #ts BETWEEN :startTs AND :endTs';
    baseParams.ExpressionAttributeNames = { '#ts': 'timestamp' };
    baseParams.ExpressionAttributeValues[':startTs'] = startTimestamp;
    baseParams.ExpressionAttributeValues[':endTs'] = endTimestamp;
  }

  // 4. --- “Saltar” páginas previas iterando hasta la página solicitada ---
  let exclusiveStartKey = undefined;
  try {
    for (let i = 0; i < page; i++) {
      // Cada iteración obtiene la página i (pero no la devolvemos; solo extraemos LastEvaluatedKey)
      const interimParams = {
        ...baseParams,
        ExclusiveStartKey: exclusiveStartKey,
      };
      const interimData = await docClient.send(new QueryCommand(interimParams));

      if (interimData.LastEvaluatedKey) {
        exclusiveStartKey = interimData.LastEvaluatedKey;
      } else {
        // Si no hay más páginas y aún no llegamos a `page`, devolvemos resultados vacíos
        return {
          statusCode: 200,
          headers: {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
          },
          body: JSON.stringify({
            items: [],
            count: 0,
            page,
            nextPage: null,
          }),
        };
      }
    }
  } catch (error) {
    console.error("Error iterando páginas previas:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Error interno al paginar en DynamoDB.",
        error: error.message,
      }),
    };
  }

  // 5. --- Ejecutar la Consulta para la “página actual” ---
  try {
    const currentParams = {
      ...baseParams,
      ExclusiveStartKey: exclusiveStartKey, // undefined si page = 0
    };

    console.log("Parámetros de la consulta DynamoDB (página " + page + "):", JSON.stringify(currentParams, null, 2));

    const data = await docClient.send(new QueryCommand(currentParams));

    // 6. --- Preparar información sobre siguiente página ---
    let nextPage = null;
    if (data.LastEvaluatedKey) {
      nextPage = page + 1;
    }

    // 7. --- Devolver Respuesta Exitosa ---
    return {
      statusCode: 200,
      headers: {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": "*",
      },
      body: JSON.stringify({
        items: data.Items || [],
        count: data.Count || 0,
        page,
        nextPage, // null si ya no hay más páginas
      }),
    };

  } catch (error) {
    console.error("Error al consultar DynamoDB en la página solicitada:", error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        message: "Error interno del servidor al consultar la base de datos.",
        error: error.message,
      }),
    };
  }
};