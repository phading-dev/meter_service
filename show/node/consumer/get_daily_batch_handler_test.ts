import { BIGTABLE } from "../../../common/bigtable";
import { GetDailyBatchHandler } from "./get_daily_batch_handler";
import { GET_DAILY_BATCH_RESPONSE } from "@phading/product_meter_service_interface/show/node/consumer/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "GetDailyBatchHandlerTest",
  cases: [
    {
      name: "GetFirstBatch_GetSecondBatch_GetLastEmptyBatch",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t1#2024-10-30#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t1#2024-10-31#consumer2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t1#2024-11-01#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t1#2024-10-26#consumer2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t1#2024-10-26#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
        ]);
        // Now is 2024-11-01 10:xx:xx UTC
        let handler = new GetDailyBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1730455658000),
        );

        {
          // Execute
          let response = await handler.handle("", {});

          // Verify
          assertThat(
            response,
            eqMessage(
              {
                rowKeys: ["t1#2024-10-26#consumer1", "t1#2024-10-26#consumer2"],
                cursor: "t1#2024-10-26#consumer2",
              },
              GET_DAILY_BATCH_RESPONSE,
            ),
            "response",
          );
        }

        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t1#2024-10-26#consumer2",
          });

          // Verify
          assertThat(
            response,
            eqMessage(
              {
                rowKeys: ["t1#2024-10-30#consumer1", "t1#2024-10-31#consumer2"],
                cursor: "t1#2024-10-31#consumer2",
              },
              GET_DAILY_BATCH_RESPONSE,
            ),
            "response 2",
          );
        }

        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t1#2024-10-31#consumer2",
          });

          // Verify
          assertThat(
            response,
            eqMessage(
              {
                rowKeys: [],
              },
              GET_DAILY_BATCH_RESPONSE,
            ),
            "response 3",
          );
        }
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
      },
    },
    {
      name: "SkipYesterdayDueToTimezone",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t1#2024-10-30#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t1#2024-10-31#consumer2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t1#2024-11-01#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
        ]);
        // Now is 2024-11-01 07:xx:xx UTC
        let handler = new GetDailyBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1730444400000),
        );

        // Execute
        let response = await handler.handle("", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: ["t1#2024-10-30#consumer1"],
            },
            GET_DAILY_BATCH_RESPONSE,
          ),
          "response",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
      },
    },
  ],
});
