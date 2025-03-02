import "../../../local/env";
import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessDailyStorageReadingHandler } from "./process_daily_storage_reading_handler";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ProcessDailyStorageReadingHandlerTest",
  cases: [
    {
      name: "Process",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t6#2024-10-31#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d6#2024-10-31#publisher1",
            data: {
              u: {
                file1: {
                  value: 1000,
                },
                file2: {
                  value: 2000,
                },
              },
              s: {
                "video1#b": {
                  value: 10330,
                },
                "video1#s": {
                  value: 1730361600000, // 2024-10-31T08:00:00Z
                },
                "video2#b": {
                  value: 20580,
                },
                "video2#s": {
                  value: 1730376000000, // 2024-10-31T12:00:00Z
                },
                "video2#e": {
                  value: 1730379600000, // 2024-10-31T13:00:00Z
                },
                "video3#s": {
                  value: 1730376000000, // 2024-10-31T12:00:00Z
                },
                "video3#b": {
                  value: 30870,
                },
              },
            },
          },
        ]);
        let handler = new ProcessDailyStorageReadingHandler(BIGTABLE);

        // Execute
        await handler.handle("", {
          rowKey: "t6#2024-10-31#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-31").get())[0].data,
          eqData({
            t: {
              uk: {
                value: 3,
              },
              smm: {
                value: 53,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#31").get())[0].data,
          eqData({
            t: {
              uk: {
                value: 3,
              },
              smm: {
                value: 53,
              },
            },
          }),
          "temp month data",
        );
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher1").exists())[0],
          eq(true),
          "month task added",
        );
        assertThat(
          (await BIGTABLE.row("d6#2024-11-01#publisher1").get())[0].data,
          eqData({
            s: {
              "video1#b": {
                value: 10330,
              },
              "video1#s": {
                value: 1730448000000, // 2024-11-01T08:00:00Z
              },
              "video3#s": {
                value: 1730448000000, // 2024-11-01T08:00:00Z
              },
              "video3#b": {
                value: 30870,
              },
            },
          }),
          "carry over data",
        );
        assertThat(
          (await BIGTABLE.row("t6#2024-11-01#publisher1").exists())[0],
          eq(true),
          "carry over task added",
        );
        assertThat(
          (await BIGTABLE.row("t6#2024-10-30#publisher1").exists())[0],
          eq(false),
          "task deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "UploadsOnly",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t6#2024-10-31#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d6#2024-10-31#publisher1",
            data: {
              u: {
                file1: {
                  value: 1000,
                },
                file2: {
                  value: 2000,
                },
              },
            },
          },
        ]);
        let handler = new ProcessDailyStorageReadingHandler(BIGTABLE);

        // Execute
        await handler.handle("", {
          rowKey: "t6#2024-10-31#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-31").get())[0].data,
          eqData({
            t: {
              uk: {
                value: 3,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#31").get())[0].data,
          eqData({
            t: {
              uk: {
                value: 3,
              },
            },
          }),
          "temp month data",
        );
        assertThat(
          (await BIGTABLE.row("d6#2024-11-01#publisher1").exists())[0],
          eq(false),
          "carry over data",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "StorageOnly",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t6#2024-10-31#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d6#2024-10-31#publisher1",
            data: {
              s: {
                "video1#b": {
                  value: 10330,
                },
                "video1#s": {
                  value: 1730361600000, // 2024-10-31T08:00:00Z
                },
              },
            },
          },
        ]);
        let handler = new ProcessDailyStorageReadingHandler(BIGTABLE);

        // Execute
        await handler.handle("", {
          rowKey: "t6#2024-10-31#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-31").get())[0].data,
          eqData({
            t: {
              smm: {
                value: 15,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#31").get())[0].data,
          eqData({
            t: {
              smm: {
                value: 15,
              },
            },
          }),
          "temp month data",
        );
        assertThat(
          (await BIGTABLE.row("d6#2024-11-01#publisher1").get())[0].data,
          eqData({
            s: {
              "video1#b": {
                value: 10330,
              },
              "video1#s": {
                value: 1730448000000, // 2024-11-01T08:00:00Z
              },
            },
          }),
          "carry over data",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "AllStorageEndedAndSomeWithNegativePeriod",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t6#2024-10-31#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d6#2024-10-31#publisher1",
            data: {
              s: {
                "video1#b": {
                  value: 10330,
                },
                "video1#s": {
                  value: 1730361600000, // 2024-10-31T08:00:00Z
                },
                "video1#e": {
                  value: 1730383200000, // 2024-10-31T14:00:00Z
                },
                "video2#b": {
                  value: 20580,
                },
                "video2#s": {
                  value: 1730376000000, // 2024-10-31T12:00:00Z
                },
                "video2#e": {
                  value: 1730379600000, // 2024-10-31T13:00:00Z
                },
                "video3#s": {
                  value: 1730376000000, // 2024-10-31T12:00:00Z
                },
                "video3#b": {
                  value: 30870,
                },
                "video3#e": {
                  value: 1730282400000, // 2024-10-30T10:00:00Z
                },
              },
            },
          },
        ]);
        let handler = new ProcessDailyStorageReadingHandler(BIGTABLE);

        // Execute
        await handler.handle("", {
          rowKey: "t6#2024-10-31#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-31").get())[0].data,
          eqData({
            t: {
              smm: {
                value: -39,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#31").get())[0].data,
          eqData({
            t: {
              smm: {
                value: -39,
              },
            },
          }),
          "temp month data",
        );
        assertThat(
          (await BIGTABLE.row("d6#2024-11-01#publisher1").exists())[0],
          eq(false),
          "carry over data",
        );
        assertThat(
          (await BIGTABLE.row("t6#2024-11-01#publisher1").exists())[0],
          eq(false),
          "carry over task",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "WithoutDataRow",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t6#2024-10-31#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
        ]);
        let handler = new ProcessDailyStorageReadingHandler(BIGTABLE);

        // Execute
        await handler.handle("", {
          rowKey: "t6#2024-10-31#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("t6#2024-10-31#publisher1").exists())[0],
          eq(false),
          "task deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
