import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { RecordStorageEndHandler } from "./record_storage_end_handler";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "RecordStorageEndHandlerTest",
  cases: [
    {
      name: "Success",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "d6#2024-11-26#publisher1",
            data: {
              s: {
                "videoFile#b": {
                  value: 1000,
                },
                "videoFile#s": {
                  value: 1732608800000,
                },
              },
            },
          },
        ]);
        // 2024-11-26T11:00:00Z
        let handler = new RecordStorageEndHandler(
          BIGTABLE,
          () => new Date(1732618800000),
        );

        // Execute
        await handler.handle("", {
          accountId: "publisher1",
          name: "videoFile",
          storageEndMs: 1732612800000,
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
            s: {
              "videoFile#b": {
                value: 1000,
              },
              "videoFile#s": {
                value: 1732608800000,
              },
              "videoFile#e": {
                value: 1732612800000,
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
