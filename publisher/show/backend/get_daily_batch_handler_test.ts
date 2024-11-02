import { BIGTABLE } from "../../../common/bigtable";
import { GetDailyBatchHandler } from "./get_daily_batch_handler";
import { GET_DAILY_BATCH_RESPONSE } from "@phading/product_meter_service_interface/publisher/show/backend/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "GetDailyBatchHandlerTest",
  cases: [
    {
      name: "GetFirstBatch_GetSecondBatch_GetLastEmptyBatchExcludingUnprocessedDate",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t4#2024-10-28#publisher1",
            data: {
              h: {
                e: {
                  value: 1,
                },
              },
            },
          },
          {
            key: "t4#2024-10-28#publisher2",
            data: {
              h: {
                e: {
                  value: 1,
                },
              },
            },
          },
          {
            key: "t4#2024-10-28#publisher3",
            data: {
              h: {
                e: {
                  value: 1,
                },
              },
            },
          },
          {
            key: "t4#2024-10-29#publisher1",
            data: {
              h: {
                e: {
                  value: 1,
                },
              },
            },
          },
          {
            key: "t4#2024-10-30#publisher1",
            data: {
              h: {
                e: {
                  value: 1,
                },
              },
            },
          },
          {
            key: "t4#2024-10-30#publisher2",
            data: {
              h: {
                e: {
                  value: 1,
                },
              },
            },
          },
          {
            key: "t1#2024-10-30#consumer1",
            data: {
              w: {
                "season1#ep1": {
                  value: 10,
                },
              },
            },
          },
        ]);
        // 2024-11-01 10:xx:xx UTC
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
                rowKeys: [
                  "t4#2024-10-28#publisher1",
                  "t4#2024-10-28#publisher2",
                ],
                cursor: "t4#2024-10-28#publisher2",
              },
              GET_DAILY_BATCH_RESPONSE,
            ),
            "response 1st",
          );
        }

        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t4#2024-10-28#publisher2",
          });

          // Verify
          assertThat(
            response,
            eqMessage(
              {
                rowKeys: [
                  "t4#2024-10-28#publisher3",
                  "t4#2024-10-29#publisher1",
                ],
                cursor: "t4#2024-10-29#publisher1",
              },
              GET_DAILY_BATCH_RESPONSE,
            ),
            "response 2nd",
          );
        }

        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t4#2024-10-29#publisher1",
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
            key: "t4#2024-10-28#publisher1",
            data: {
              h: {
                e: {
                  value: 1,
                },
              },
            },
          },
          {
            key: "t4#2024-11-01#publisher1",
            data: {
              h: {
                e: {
                  value: 1,
                },
              },
            },
          },
          {
            key: "t1#2024-11-02#consumer1",
            data: {
              w: {
                "season1#ep1": {
                  value: 10,
                },
              },
            },
          },
        ]);
        // 2024-11-01 10:xx:xx UTC
        let handler = new GetDailyBatchHandler(
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
              rowKeys: ["t4#2024-10-28#publisher1"],
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
