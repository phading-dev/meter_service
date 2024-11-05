import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessDailyMeterReadingHandler } from "../../../publisher/show/backend/process_daily_meter_reading_handler";
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
      key: `t4#2024-10-30#publisher1`,
      data: {
        t: {
          w: {
            value: 0,
          },
          kb: {
            value: 0,
          },
        },
        c: {
          r: {
            value: "",
          },
          p: {
            value: "",
          },
        },
      },
    },
    {
      key: `t3#2024-10-30#publisher1#consumer1`,
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
      key: `t3#2024-10-30#publisher1#consumer2`,
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
      key: `t3#2024-10-30#publisher1#consumer3`,
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
      key: `t3#2024-10-30#publisher1#consumer4`,
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
      key: `t3#2024-10-30#publisher2#consumer4`,
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
        let handler = new ProcessDailyMeterReadingHandler(2, BIGTABLE);

        // Execute
        await handler.handle("", {
          rowKey: "t4#2024-10-30#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f2#publisher1#2024-10-30").get())[0].data,
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
          (await BIGTABLE.row("t5#2024-10#publisher1#30").get())[0].data,
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
          (await BIGTABLE.row(`t4#2024-10-30#publisher1`).exists())[0],
          eq(false),
          "original row deleted",
        );
        assertThat(
          (
            await BIGTABLE.row(`t3#2024-10-30#publisher1#consumer1`).exists()
          )[0],
          eq(false),
          "one of data row deleted",
        );
        assertThat(
          (
            await BIGTABLE.row(`t3#2024-10-30#publisher2#consumer4`).exists()
          )[0],
          eq(true),
          "extra data row exists",
        );
      },
      tearDown: async () => {
        await Promise.all([BIGTABLE.deleteRows("t"), BIGTABLE.deleteRows("f")]);
      },
    },
    {
      name: "InterruptEveryAggregation_InterrutpAfterFinalWrite_ResumeAndMarkDone_ResumeWithNoAction",
      execute: async () => {
        // Prepare
        await initData();
        let aggreagtionError: Error;
        let finalWriteError: Error;
        let handler = new ProcessDailyMeterReadingHandler(
          2,
          BIGTABLE,
          () => {
            if (aggreagtionError) {
              throw aggreagtionError;
            }
          },
          () => {
            if (finalWriteError) {
              throw finalWriteError;
            }
          },
        );
        aggreagtionError = new Error("fake agg");

        // Execute
        let error = await assertReject(
          handler.handle("", {
            rowKey: "t4#2024-10-30#publisher1",
          }),
        );

        // Verify
        assertThat(error.message, containStr("fake agg"), "interrupted 1");
        assertThat(
          (await BIGTABLE.row("t4#2024-10-30#publisher1").get())[0].data,
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
            c: {
              r: {
                value: "t3#2024-10-30#publisher1#consumer2",
              },
              p: {
                value: "",
              },
            },
          }),
          "checkpoint 1",
        );

        // Execute
        error = await assertReject(
          handler.handle("", {
            rowKey: "t4#2024-10-30#publisher1",
          }),
        );

        // Verify
        assertThat(
          (await BIGTABLE.row("t4#2024-10-30#publisher1").get())[0].data,
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
            c: {
              r: {
                value: "t3#2024-10-30#publisher1#consumer4",
              },
              p: {
                value: "",
              },
            },
          }),
          "checkpoint 2",
        );

        // Execute
        error = await assertReject(
          handler.handle("", {
            rowKey: "t4#2024-10-30#publisher1",
          }),
        );

        // Verify
        assertThat(
          (await BIGTABLE.row("t4#2024-10-30#publisher1").get())[0].data,
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
            c: {
              r: {
                value: "",
              },
              p: {
                value: "1",
              },
            },
          }),
          "checkpoint 3",
        );

        // Prepare
        finalWriteError = new Error("fake write");

        // Execute
        error = await assertReject(
          handler.handle("", {
            rowKey: "t4#2024-10-30#publisher1",
          }),
        );

        // Verify
        assertThat(error.message, containStr("fake write"), "write error");
        let finalPublisherData: any = {
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
        };
        assertThat(
          (await BIGTABLE.row("f2#publisher1#2024-10-30").get())[0].data,
          eqData(finalPublisherData),
          "final publisher data",
        );
        let tempMonthPublisherData: any = {
          t: {
            w: {
              value: 59,
            },
            kb: {
              value: 52,
            },
          },
        };
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher1#30").get())[0].data,
          eqData(tempMonthPublisherData),
          "temp month publisher data",
        );
        assertThat(
          (
            await BIGTABLE.row(`t3#2024-10-30#publisher1#consumer1`).exists()
          )[0],
          eq(false),
          "one of data row deleted",
        );
        assertThat(
          (await BIGTABLE.row(`t4#2024-10-30#publisher1`).exists())[0],
          eq(true),
          "original row exists",
        );

        // Prepare
        finalWriteError = undefined;

        // Execute
        await handler.handle("", {
          rowKey: "t4#2024-10-30#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f2#publisher1#2024-10-30").get())[0].data,
          eqData(finalPublisherData),
          "final publisher data",
        );
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher1#30").get())[0].data,
          eqData(tempMonthPublisherData),
          "temp month publisher data",
        );
        assertThat(
          (await BIGTABLE.row(`t4#2024-10-30#publisher1`).exists())[0],
          eq(false),
          "original row deleted",
        );

        // Execute
        await handler.handle("", {
          rowKey: "t4#2024-10-30#publisher1",
        });

        // Verify no action and no error
      },
      tearDown: async () => {
        await Promise.all([BIGTABLE.deleteRows("t"), BIGTABLE.deleteRows("f")]);
      },
    },
  ],
});
