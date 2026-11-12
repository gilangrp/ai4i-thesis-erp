import http from "k6/http";
import { check } from "k6";

const baseUrl = (__ENV.BASE_URL || "http://localhost:8000").replace(/\/+$/, "");

export const options = {
  stages: [
    { duration: "30s", target: 10 },
    { duration: "1m", target: 10 },
    { duration: "30s", target: 0 },
  ],
};

const headers = { "Content-Type": "application/json" };

const singlePayload = {
  type: "L",
  air_temp_k: 300.5,
  process_temp_k: 310.9,
  rpm: 1500,
  torque_nm: 55.0,
  tool_wear_min: 215,
};

const batchPayload = {
  items: [
    {
      type: "L",
      air_temp_k: 298.1,
      process_temp_k: 308.6,
      rpm: 1551,
      torque_nm: 42.8,
      tool_wear_min: 0,
    },
    singlePayload,
  ],
};

export default function () {
  const singleResponse = http.post(
    `${baseUrl}/predict`,
    JSON.stringify(singlePayload),
    { headers, tags: { endpoint: "predict" } },
  );
  check(singleResponse, {
    "POST /predict returns 2xx": (response) =>
      response.status >= 200 && response.status < 300,
  });

  const batchResponse = http.post(
    `${baseUrl}/predict/batch`,
    JSON.stringify(batchPayload),
    { headers, tags: { endpoint: "predict_batch" } },
  );
  check(batchResponse, {
    "POST /predict/batch returns 2xx": (response) =>
      response.status >= 200 && response.status < 300,
  });
}

export function handleSummary(data) {
  const requestCount = data.metrics.http_reqs.values.count;
  const averageLatencyMs = data.metrics.http_req_duration.values.avg;
  const durationMs = data.state.testRunDurationMs;
  const throughputPerMinute =
    durationMs > 0 ? (requestCount / durationMs) * 60_000 : 0;

  return {
    stdout: [
      "\nSummary load test",
      `Request Count: ${requestCount}`,
      `Latency Average: ${averageLatencyMs.toFixed(2)} ms`,
      `Throughput: ${throughputPerMinute.toFixed(2)} request/menit`,
      "",
    ].join("\n"),
  };
}
