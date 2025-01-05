import { BIGTABLE } from "../../../common/bigtable";
import { GetDailyWatchBatchHandler } from "./get_daily_watch_batch_handler";
import { GET_DAILY_WATCH_BATCH_RESPONSE } from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "GetDailyWatchBatchHandlerTest",
  cases: [
    {
      name: "GetFirstBatch_GetSecondBatch_GetLastEmptyBatchExcludingUnprocessedDate",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t3#2024-10-28#publisher1",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t3#2024-10-28#publisher2",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t3#2024-10-28#publisher2#checkpoint1",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t3#2024-10-29#publisher1",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t3#2024-10-30#publisher2",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
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
        ]);
        // 2024-11-01 10:xx:xx UTC
        let handler = new GetDailyWatchBatchHandler(
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
                rowKeys: [
                  "t3#2024-10-28#publisher1",
                  "t3#2024-10-28#publisher2",
                ],
                cursor: "t3#2024-10-28#publisher2",
              },
              GET_DAILY_WATCH_BATCH_RESPONSE,
            ),
            "response 1st",
          );
        }

        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t3#2024-10-28#publisher2",
          });

          // Verify
          assertThat(
            response,
            eqMessage(
              {
                rowKeys: [
                  "t3#2024-10-28#publisher2#checkpoint1",
                  "t3#2024-10-29#publisher1",
                ],
                cursor: "t3#2024-10-29#publisher1",
              },
              GET_DAILY_WATCH_BATCH_RESPONSE,
            ),
            "response 2nd",
          );
        }

        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t3#2024-10-29#publisher1",
          });

          // Verify
          assertThat(
            response,
            eqMessage(
              {
                rowKeys: [],
              },
              GET_DAILY_WATCH_BATCH_RESPONSE,
            ),
            "response 3rd",
          );
        }
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
      },
    },
    {
      name: "GetUntilToday",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t3#2024-10-28#publisher1",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t3#2024-11-01#publisher1",
            data: {
              c: {
                r: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t1#2024-11-02#consumer1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
        ]);
        // 2024-11-01 10:xx:xx UTC
        let handler = new GetDailyWatchBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1730455658000),
        );

        // Execute
        let response = await handler.handle("", {});

        // Verify
        assertThat(
          response,
          eqMessage(
            {
              rowKeys: ["t3#2024-10-28#publisher1"],
            },
            GET_DAILY_WATCH_BATCH_RESPONSE,
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
