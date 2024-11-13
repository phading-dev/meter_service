import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessDailyMeterReadingHandler } from "./process_daily_meter_reading_handler";
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
      key: `q3#2024-10-30#publisher1`,
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
          kb: {
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
          kb: {
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
          kb: {
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
          kb: {
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
          kb: {
            value: 100,
          },
        },
      },
    },
  ]);
}

TEST_RUNNER.run({
  name: "ProcessDailyMeterReadingHandlerTest",
  cases: [
    {
      name: "CompleteProcessingInOneShot",
      execute: async () => {
        // Prepare
        await initData();
        let id = 1;
        let handler = new ProcessDailyMeterReadingHandler(
          2,
          BIGTABLE,
          () => `checkpoint${id++}`,
        );

        // Execute
        await handler.handle("", {
          rowKey: "q3#2024-10-30#publisher1",
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
              w: {
                value: 59,
              },
              kb: {
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
              w: {
                value: 59,
              },
              kb: {
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
              w: {
                value: 54,
              },
              kb: {
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
              w: {
                value: 59,
              },
              kb: {
                value: 52,
              },
            },
          }),
          "checkpoint 2 data",
        );
        assertThat(
          (await BIGTABLE.row(`q3#2024-10-30#publisher1`).exists())[0],
          eq(false),
          "queue deleted",
        );
        assertThat(
          (
            await BIGTABLE.row(`q3#2024-10-30#publisher1#checkpoint1`).exists()
          )[0],
          eq(false),
          "checkpoint 1 queue deleted",
        );
        assertThat(
          (
            await BIGTABLE.row(`q3#2024-10-30#publisher1#checkpoint2`).exists()
          )[0],
          eq(false),
          "checkpoint 1 queue deleted",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
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
        let handler = new ProcessDailyMeterReadingHandler(
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
            rowKey: "q3#2024-10-30#publisher1",
          }),
        );

        // Verify
        assertThat(error.message, containStr("fake error"), "interrupted 1");
        assertThat(
          (await BIGTABLE.row("q3#2024-10-30#publisher1#checkpoint1").get())[0]
            .data,
          eqData({
            c: {
              r: {
                value: "d3#2024-10-30#publisher1#consumer2",
              },
            },
          }),
          "checkpoint 1 enqueued",
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
              w: {
                value: 54,
              },
              kb: {
                value: 49,
              },
            },
          }),
          "checkpoint 1 data",
        );
        assertThat(
          (await BIGTABLE.row(`q3#2024-10-30#publisher1`).exists())[0],
          eq(false),
          "original queue deleted",
        );

        // Execute
        error = await assertReject(
          handler.handle("", {
            rowKey: "q3#2024-10-30#publisher1#checkpoint1",
          }),
        );

        // Verify
        assertThat(
          (await BIGTABLE.row("q3#2024-10-30#publisher1#checkpoint2").get())[0]
            .data,
          eqData({
            c: {
              r: {
                value: "d3#2024-10-30#publisher1#consumer4",
              },
            },
          }),
          "checkpoint 2 enqueued",
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
              w: {
                value: 59,
              },
              kb: {
                value: 52,
              },
            },
          }),
          "checkpoint 2 data",
        );
        assertThat(
          (
            await BIGTABLE.row(`q3#2024-10-30#publisher1#checkpoint1`).exists()
          )[0],
          eq(false),
          "checkpoint 1 queue deleted",
        );

        // Prepare
        handler.interruptAfterCheckPoint = () => {};

        // Execute
        await handler.handle("", {
          rowKey: "q3#2024-10-30#publisher1#checkpoint2",
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
              w: {
                value: 59,
              },
              kb: {
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
              w: {
                value: 59,
              },
              kb: {
                value: 52,
              },
            },
          }),
          "temp month publisher data",
        );
        assertThat(
          (
            await BIGTABLE.row(`q3#2024-10-30#publisher1#checkpoint2`).exists()
          )[0],
          eq(false),
          "checkpoint 2 queue deleted",
        );

        // Execute
        await handler.handle("", {
          rowKey: "q3#2024-10-30#publisher1#checkpoint2",
        });

        // Verify no action and no error
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("q");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
  ],
});
