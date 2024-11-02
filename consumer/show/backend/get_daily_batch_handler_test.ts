import { BIGTABLE } from "../../../common/bigtable";
import { GetDailyBatchHandler } from "./get_daily_batch_handler";
import { GET_DAILY_BATCH_RESPONSE } from "@phading/product_meter_service_interface/consumer/show/backend/interface";
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
              w: {
                "season3#ep3": {
                  value: 1000,
                },
                "season1#ep1": {
                  value: 1000,
                },
              },
            },
          },
          {
            key: "t1#2024-10-30#consumer2",
            data: {
              w: {
                "season3#ep3": {
                  value: 1000,
                },
              },
            },
          },
          {
            key: "t1#2024-11-01#consumer1",
            data: {
              w: {
                "season4#ep3": {
                  value: 1000,
                },
              },
            },
          },
          {
            key: "t1#2024-10-26#consumer2",
            data: {
              w: {
                "season1#ep2": {
                  value: 300,
                },
                "season2#ep1": {
                  value: 300,
                },
              },
            },
          },
          {
            key: "t1#2024-10-26#consumer1",
            data: {
              w: {
                "season1#ep1": {
                  value: 200,
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
                rowKeys: ["t1#2024-10-30#consumer1", "t1#2024-10-30#consumer2"],
                cursor: "t1#2024-10-30#consumer2",
              },
              GET_DAILY_BATCH_RESPONSE,
            ),
            "response 2",
          );
        }

        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t1#2024-10-30#consumer2",
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
        await BIGTABLE.deleteRows("t1#");
      },
    },
  ],
});
