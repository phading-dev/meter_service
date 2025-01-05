import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessDailyWatchReadingHandler } from "./process_daily_watch_reading_handler";
import {
  assertReject,
  assertThat,
  containStr,
  eq,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

async function initData() {
  await BIGTABLE.insert([
    {
      key: `t3#2024-10-30#publisher1`,
      data: {
        c: {
          r: {
            value: "",
          },
        },
      },
    },
    {
      key: `d3#2024-10-30#publisher1#consumer1`,
      data: {
        w: {
          season1: {
            value: 1,
          },
          season2: {
            value: 12,
          },
        },
        a: {
          season1: {
            value: 3,
          },
          season2: {
            value: 40,
          },
        },
        t: {
          nk: {
            value: 17,
          },
        },
      },
    },
    {
      key: `d3#2024-10-30#publisher1#consumer2`,
      data: {
        w: {
          season1: {
            value: 3,
          },
          season3: {
            value: 5,
          },
        },
        a: {
          season1: {
            value: 1,
          },
          season3: {
            value: 10,
          },
        },
        t: {
          nk: {
            value: 32,
          },
        },
      },
    },
    {
      key: `d3#2024-10-30#publisher1#consumer3`,
      data: {
        w: {
          season1: {
            value: 1,
          },
        },
        a: {
          season1: {
            value: 2,
          },
        },
        t: {
          nk: {
            value: 2,
          },
        },
      },
    },
    {
      key: `d3#2024-10-30#publisher1#consumer4`,
      data: {
        w: {
          season1: {
            value: 2,
          },
        },
        a: {
          season1: {
            value: 3,
          },
        },
        t: {
          nk: {
            value: 1,
          },
        },
      },
    },
    {
      key: `d3#2024-10-30#publisher2#consumer4`,
      data: {
        w: {
          season1: {
            value: 1000,
          },
        },
        a: {
          season1: {
            value: 3000,
          },
        },
        t: {
          nk: {
            value: 100,
          },
        },
      },
    },
  ]);
}

TEST_RUNNER.run({
  name: "ProcessDailyWatchReadingHandlerTest",
  cases: [
    {
      name: "CompleteProcessingInOneShot",
      execute: async () => {
        // Prepare
        await initData();
        let id = 1;
        let handler = new ProcessDailyWatchReadingHandler(
          2,
          BIGTABLE,
          () => `checkpoint${id++}`,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t3#2024-10-30#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-30").get())[0].data,
          eqData({
            w: {
              season1: {
                value: 7,
              },
              season2: {
                value: 12,
              },
              season3: {
                value: 5,
              },
            },
            a: {
              season1: {
                value: 9,
              },
              season2: {
                value: 40,
              },
              season3: {
                value: 10,
              },
            },
            t: {
              ws: {
                value: 59,
              },
              nk: {
                value: 52,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#30").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 59,
              },
              nk: {
                value: 52,
              },
            },
          }),
          "temp month publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d4#2024-10-30#publisher1#checkpoint1").get())[0]
            .data,
          eqData({
            w: {
              season1: {
                value: 4,
              },
              season2: {
                value: 12,
              },
              season3: {
                value: 5,
              },
            },
            a: {
              season1: {
                value: 4,
              },
              season2: {
                value: 40,
              },
              season3: {
                value: 10,
              },
            },
            t: {
              ws: {
                value: 54,
              },
              nk: {
                value: 49,
              },
            },
          }),
          "checkpoint 1 data",
        );
        assertThat(
          (await BIGTABLE.row("d4#2024-10-30#publisher1#checkpoint2").get())[0]
            .data,
          eqData({
            w: {
              season1: {
                value: 7,
              },
              season2: {
                value: 12,
              },
              season3: {
                value: 5,
              },
            },
            a: {
              season1: {
                value: 9,
              },
              season2: {
                value: 40,
              },
              season3: {
                value: 10,
              },
            },
            t: {
              ws: {
                value: 59,
              },
              nk: {
                value: 52,
              },
            },
          }),
          "checkpoint 2 data",
        );
        assertThat(
          (await BIGTABLE.row(`t5#2024-10#publisher1`).exists())[0],
          eq(true),
          "publisher month task added",
        );
        assertThat(
          (await BIGTABLE.row(`t3#2024-10-30#publisher1`).exists())[0],
          eq(false),
          "task deleted",
        );
        assertThat(
          (
            await BIGTABLE.row(`t3#2024-10-30#publisher1#checkpoint1`).exists()
          )[0],
          eq(false),
          "checkpoint 1 task deleted",
        );
        assertThat(
          (
            await BIGTABLE.row(`t3#2024-10-30#publisher1#checkpoint2`).exists()
          )[0],
          eq(false),
          "checkpoint 2 task deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "InterruptEveryAggregation_ResumeAndDone_ResumeWithNoAction",
      execute: async () => {
        // Prepare
        await initData();
        let id = 1;
        let handler = new ProcessDailyWatchReadingHandler(
          2,
          BIGTABLE,
          () => `checkpoint${id++}`,
        );
        handler.interruptAfterCheckPoint = () => {
          throw new Error("fake error");
        };

        // Execute
        let error = await assertReject(
          handler.handle("", {
            rowKey: "t3#2024-10-30#publisher1",
          }),
        );

        // Verify
        assertThat(error.message, containStr("fake error"), "interrupted 1");
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher1#checkpoint1").get())[0]
            .data,
          eqData({
            c: {
              r: {
                value: "d3#2024-10-30#publisher1#consumer2",
              },
            },
          }),
          "checkpoint 1 task added",
        );
        assertThat(
          (await BIGTABLE.row("d4#2024-10-30#publisher1#checkpoint1").get())[0]
            .data,
          eqData({
            w: {
              season1: {
                value: 4,
              },
              season2: {
                value: 12,
              },
              season3: {
                value: 5,
              },
            },
            a: {
              season1: {
                value: 4,
              },
              season2: {
                value: 40,
              },
              season3: {
                value: 10,
              },
            },
            t: {
              ws: {
                value: 54,
              },
              nk: {
                value: 49,
              },
            },
          }),
          "checkpoint 1 data",
        );
        assertThat(
          (await BIGTABLE.row(`t3#2024-10-30#publisher1`).exists())[0],
          eq(false),
          "original task deleted",
        );

        // Execute
        error = await assertReject(
          handler.handle("", {
            rowKey: "t3#2024-10-30#publisher1#checkpoint1",
          }),
        );

        // Verify
        assertThat(
          (await BIGTABLE.row("t3#2024-10-30#publisher1#checkpoint2").get())[0]
            .data,
          eqData({
            c: {
              r: {
                value: "d3#2024-10-30#publisher1#consumer4",
              },
            },
          }),
          "checkpoint 2 task added",
        );
        assertThat(
          (await BIGTABLE.row("d4#2024-10-30#publisher1#checkpoint2").get())[0]
            .data,
          eqData({
            w: {
              season1: {
                value: 7,
              },
              season2: {
                value: 12,
              },
              season3: {
                value: 5,
              },
            },
            a: {
              season1: {
                value: 9,
              },
              season2: {
                value: 40,
              },
              season3: {
                value: 10,
              },
            },
            t: {
              ws: {
                value: 59,
              },
              nk: {
                value: 52,
              },
            },
          }),
          "checkpoint 2 data",
        );
        assertThat(
          (
            await BIGTABLE.row(`t3#2024-10-30#publisher1#checkpoint1`).exists()
          )[0],
          eq(false),
          "checkpoint 1 task deleted",
        );

        // Prepare
        handler.interruptAfterCheckPoint = () => {};

        // Execute
        await handler.handle("", {
          rowKey: "t3#2024-10-30#publisher1#checkpoint2",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-30").get())[0].data,
          eqData({
            w: {
              season1: {
                value: 7,
              },
              season2: {
                value: 12,
              },
              season3: {
                value: 5,
              },
            },
            a: {
              season1: {
                value: 9,
              },
              season2: {
                value: 40,
              },
              season3: {
                value: 10,
              },
            },
            t: {
              ws: {
                value: 59,
              },
              nk: {
                value: 52,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#30").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 59,
              },
              nk: {
                value: 52,
              },
            },
          }),
          "temp month publisher data",
        );
        assertThat(
          (await BIGTABLE.row(`t5#2024-10#publisher1`).exists())[0],
          eq(true),
          "publisher month task added",
        );
        assertThat(
          (
            await BIGTABLE.row(`t3#2024-10-30#publisher1#checkpoint2`).exists()
          )[0],
          eq(false),
          "checkpoint 2 task deleted",
        );

        // Execute
        await handler.handle("", {
          rowKey: "t3#2024-10-30#publisher1#checkpoint2",
        });

        // Verify no action and no error
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "ProcessWithoutNetworkBytes",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: `t3#2024-10-30#publisher1`,
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: `d3#2024-10-30#publisher1#consumer1`,
            data: {
              w: {
                season1: {
                  value: 1,
                },
                season2: {
                  value: 12,
                },
              },
              a: {
                season1: {
                  value: 3,
                },
                season2: {
                  value: 40,
                },
              },
            },
          },
        ]);
        let id = 1;
        let handler = new ProcessDailyWatchReadingHandler(
          2,
          BIGTABLE,
          () => `checkpoint${id++}`,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t3#2024-10-30#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-30").get())[0].data,
          eqData({
            w: {
              season1: {
                value: 1,
              },
              season2: {
                value: 12,
              },
            },
            a: {
              season1: {
                value: 3,
              },
              season2: {
                value: 40,
              },
            },
            t: {
              ws: {
                value: 43,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#30").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 43,
              },
            },
          }),
          "temp month publisher data",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "ProcessWithoutWatchTime",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: `t3#2024-10-30#publisher1`,
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: `d3#2024-10-30#publisher1#consumer1`,
            data: {
              t: {
                nk: {
                  value: 235,
                },
              },
            },
          },
        ]);
        let id = 1;
        let handler = new ProcessDailyWatchReadingHandler(
          2,
          BIGTABLE,
          () => `checkpoint${id++}`,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t3#2024-10-30#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-30").get())[0].data,
          eqData({
            t: {
              nk: {
                value: 235,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#30").get())[0].data,
          eqData({
            t: {
              nk: {
                value: 235,
              },
            },
          }),
          "temp month publisher data",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "ProcessWithPartialDataInEachRow",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: `t3#2024-10-30#publisher1`,
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: `d3#2024-10-30#publisher1#consumer1`,
            data: {
              w: {
                season1: {
                  value: 1,
                },
                season2: {
                  value: 12,
                },
              },
              a: {
                season1: {
                  value: 3,
                },
                season2: {
                  value: 40,
                },
              },
            },
          },
          {
            key: `d3#2024-10-30#publisher1#consumer2`,
            data: {
              t: {
                nk: {
                  value: 235,
                },
              },
            },
          },
        ]);
        let id = 1;
        let handler = new ProcessDailyWatchReadingHandler(
          2,
          BIGTABLE,
          () => `checkpoint${id++}`,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t3#2024-10-30#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f3#publisher1#2024-10-30").get())[0].data,
          eqData({
            w: {
              season1: {
                value: 1,
              },
              season2: {
                value: 12,
              },
            },
            a: {
              season1: {
                value: 3,
              },
              season2: {
                value: 40,
              },
            },
            t: {
              ws: {
                value: 43,
              },
              nk: {
                value: 235,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("d5#2024-10#publisher1#30").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 43,
              },
              nk: {
                value: 235,
              },
            },
          }),
          "temp month publisher data",
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
