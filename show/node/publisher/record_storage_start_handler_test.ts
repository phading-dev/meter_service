import "../../../local/env";
import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { RecordStorageStartHandler } from "./record_storage_start_handler";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "RecordStorageStartHandlerTest",
  cases: [
    {
      name: "Success",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "d6#2024-11-26#publisher1",
            data: {
              u: {
                file: {
                  value: 1000,
                },
              },
            },
          },
        ]);
        // 2024-11-26T11:00:00Z
        let handler = new RecordStorageStartHandler(
          BIGTABLE,
          () => new Date(1732618800000),
        );

        // Execute
        await handler.handle("", {
          accountId: "publisher1",
          name: "newVideoFile",
          storageBytes: 1100,
          storageStartMs: 1732608800000,
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("t6#2024-11-26#publisher1").exists())[0],
          eq(true),
          "task added",
        );
        assertThat(
          (await BIGTABLE.row("d6#2024-11-26#publisher1").get())[0].data,
          eqData({
            u: {
              file: {
                value: 1000,
              },
            },
            s: {
              "newVideoFile#b": {
                value: 1100,
              },
              "newVideoFile#s": {
                value: 1732608800000,
              },
            },
          }),
          "set",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("d6");
        await BIGTABLE.deleteRows("t6");
      },
    },
  ],
});
