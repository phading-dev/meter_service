import "./prod/env";
import { BIGTABLE } from "./common/bigtable";

async function main() {
  console.log(JSON.stringify((await BIGTABLE.row("d1#2025-07-28#2b7b7061-3916-4eac-8a4c-74c6f3c0e9f1").get())[0].data));
}

main();
