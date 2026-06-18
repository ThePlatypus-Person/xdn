import http from "k6/http";
import { check } from "k6";

const ENDPOINT = "/api/books";
const WRITE_RATIO = 0.2;

const RATE = 100;
const DURATION = "100s";
export const options = {
    scenarios: {
	port_2300: {
	    executor: "constant-arrival-rate",
	    rate: RATE,
	    timeUnit: "1s",
	    duration: DURATION,
	    preAllocatedVUs: 50,
	    maxVUs: 200,
	    env: { PORT: "2300" },
	    exec: "benchmark",
	},
	port_2301: {
	    executor: "constant-arrival-rate",
	    rate: RATE,
	    timeUnit: "1s",
	    duration: DURATION,
	    preAllocatedVUs: 50,
	    maxVUs: 200,
	    env: { PORT: "2301" },
	    exec: "benchmark",
	},
	port_2302: {
	    executor: "constant-arrival-rate",
	    rate: RATE,
	    timeUnit: "1s",
	    duration: DURATION,
	    preAllocatedVUs: 50,
	    maxVUs: 200,
	    env: { PORT: "2302" },
	    exec: "benchmark",
	},
    },
    thresholds: {
	http_req_duration: ["p(50)<500", "p(99)<2000"],
    },
};

// Each VU has its own counter, starts at 0
let requestCounter = 0;

export function benchmark() {
    const port = __ENV.PORT;
    const url = `http://localhost:${port}${ENDPOINT}`;
    const isWrite = Math.random() < WRITE_RATIO;

    if (isWrite) {
	requestCounter++;
	const payload = JSON.stringify({
	    title: `title${requestCounter}`,
	    author: `client${__VU}`,
	});

	const res = http.post(url, payload, {
	    headers: { "Content-Type": "application/json" },
	    tags: { type: "write", port: port },
	});

	check(res, {
	    "status is 2xx": (r) => r.status >= 200 && r.status < 300,
	});
    } else {
	const res = http.get(`${url}/1`, {
	    tags: { type: "read", port: port },
	});

	check(res, {
	    "status is 2xx": (r) => r.status >= 200 && r.status < 300,
	});
    }
}
