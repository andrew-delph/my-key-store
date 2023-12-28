// import { PodDisruptor } from "k6/x/disruptor";
import { sleep } from "k6";
import http from "k6/http";
import { check } from "k6";
import { randomString } from "https://jslib.k6.io/k6-utils/1.2.0/index.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.2/index.js";
import exec from "k6/execution";

export let options = {
  noConnectionReuse: false,
  summaryTrendStats: ["avg", "min", "med", "max"],
  // iterations: 20,
  // vus: 5,
  scenarios: {
    // load: {
    //   executor: "constant-vus",
    //   vus: 20,
    //   duration: "2h",
    //   exec: "default",
    // },
    constant_arrival: {
      executor: "constant-arrival-rate",
      // How long the test lasts
      duration: "2h",
      // How many iterations per timeUnit
      rate: 100,
      // Start `rate` iterations per second
      timeUnit: "1s",
      // Pre-allocate VUs
      preAllocatedVUs: 400,
      maxVUs: 2000,
    },
    // ramping_arrival: {
    //   executor: "ramping-arrival-rate",
    //   startRate: 50,
    //   stages: [
    //     { target: 30, duration: "2m" },
    //     { target: 300, duration: "1h" },
    //   ],
    //   // Start `rate` iterations per second
    //   timeUnit: "1s",
    //   // Pre-allocate VUs
    //   preAllocatedVUs: 50,
    //   maxVUs: 10000,
    // },
    // epoch: {
    //   executor: "constant-arrival-rate",
    //   exec: "epoch",
    //   // How long the test lasts
    //   duration: "4m",
    //   // How many iterations per timeUnit
    //   rate: 1,
    //   // Start `rate` iterations per second
    //   timeUnit: "10s",
    //   // Pre-allocate VUs
    //   preAllocatedVUs: 1,
    //   maxVUs: 1,
    // },
  },
};

let address = __ENV.ADDRESS || "localhost:8080";

export function handleSummary(data) {
  for (const key in data.metrics) {
    if (!key.includes("duration")) delete data.metrics[key];
  }

  return {
    stdout: textSummary(data, { indent: "→", enableColors: true }),
  };
}

export function basic() {
  let addr = `http://${address}`;
  let baseRes = http.get(addr);
  check(baseRes, {
    "Basic: status was 200": (r) =>
      r.status === 200 || console.error(`Base Error: Status was ${r.status}`),
  });

  return;
}

export function health() {
  let addr = `http://${address}/health`;
  let baseRes = http.get(addr);
  check(baseRes, {
    "Health: status was 200": (r) => r.status === 200,
  });

  if (baseRes.status != 200) {
    console.error(`Status:`, baseRes.status);
  }

  return;
}

export function panic() {
  let addr = `http://${address}/panic`;
  let baseRes = http.get(addr);
  check(baseRes, {
    "Panic: status was 200": (r) =>
      r.status === 200 || console.error(`Base Error: Status was ${r.status}`),
  });

  return;
}

export function leader() {
  let addr = `http://${address}/leader`;
  let baseRes = http.get(addr);
  check(baseRes, {
    "Leader: status was 200": (r) =>
      r.status === 200 || console.error(`Base Error: Status was ${r.status}`),
  });

  return;
}

export function follower() {
  let addr = `http://${address}/follower`;
  console.log(addr);
  let baseRes = http.get(addr);
  check(baseRes, {
    "Follower: status was 200": (r) =>
      r.status === 200 || console.error(`Base Error: Status was ${r.status}`),
  });

  return;
}

export function bootstrap() {
  let addr = `http://${address}/bootstrap`;

  let baseRes = http.get(addr);
  check(baseRes, {
    "bootstrap: status was 200": (r) =>
      r.status === 200 ||
      console.error(`bootstrap Error: Status was ${r.status}`),
  });

  return;
}

export function epoch() {
  let addr = `http://${address}/epoch`;

  let baseRes = http.get(addr);
  check(baseRes, {
    "epoch: status was 200": (r) =>
      r.status === 200 || console.error(`epoch Error: Status was ${r.status}`),
  });
  return;
}

export function remove() {
  let addr = `http://${address}/remove`;

  let baseRes = http.get(addr);
  check(baseRes, {
    "remove: status was 200": (r) =>
      r.status === 200 || console.error(`remove Error: Status was ${r.status}`),
  });
  return;
}

// options = { duration: "2h", vus: 1 };
options = { iterations: 10000, vus: 500 };
export default function () {
  const iterationNumber = exec.scenario.iterationInTest;

  // the key value to insert
  let set_key = randomString(5);
  let set_value = randomString(5);
  // key = "test!";
  // value = "123000";

  // Set a value to the map
  let setRes = http.get(
    `http://${address}/set?key=${set_key}&value=${set_value}`,
    {
      tags: { name: "set" },
    }
  );
  check(setRes, {
    "set status is 200": (r) => r.status === 200,
  });
  return;

  if (setRes.status != 200) {
    console.error("SET REQUEST FAILED:", setRes.status, setRes.body);
    return;
  }
  // while (true) {
  for (let i = 0; i < 10; i++) {
    // Get a value from the map
    let getRes = http.get(`http://${address}/get?key=${set_key}`, {
      tags: { name: "get" },
    });
    const set_members = setRes.json("Members") || [];
    const get_members = getRes.json("Failed_members") || [];
    const get_value = getRes.json("Value");
    const get_error = getRes.json("Error") || getRes.body;
    set_members.sort();
    get_members.sort();

    check(getRes, {
      "get status is 200": (r) => r.status === 200,
      "get the correct value": (r) => r.json("Value") === set_value,
      // "get the correct value": (r) =>
      //   r.json("Value") === value ||
      //   console.error(
      //     "get:",
      //     r.status,
      //     `${r.body}`.replace(/\n/g, ""),
      //     iterationNumber
      //   ),
    });
    if (getRes.status != 200) {
      const diff = get_members.filter((item) => !set_members.includes(item));
      console.log(
        "i",
        i,
        "status",
        getRes.status,
        "diff",
        diff,
        "get_members",
        get_members.length,
        "set_value",
        set_value,
        "get_value",
        get_value,
        "err",
        get_error
      );
    }
    sleep(1);
  }
}
