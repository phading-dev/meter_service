import { BIGTABLE } from "../../../common/bigtable";
import { GetDailyStorageBatchHandler } from "./get_daily_storage_batch_handler";
import { GET_DAILY_STORAGE_BATCH_RESPONSE } from "@phading/product_meter_service_interface/show/node/publisher/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { assertThat } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "GetDailyStorageBatchHandlerTest",
  cases: [
    {
      name: "SkipToday_FirstBatch_SecondBatch_LastEmptyBatch",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t6#2024-10-30#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t6#2024-10-31#publisher2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t6#2024-11-01#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t6#2024-10-26#publisher2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t6#2024-10-26#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
        ]);
        // Now is 2024-11-01T09:00:00Z
        let handler = new GetDailyStorageBatchHandler(
          2,
          BIGTABLE,
          () => new Date(1730451600000),
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
                  "t6#2024-10-26#publisher1",
                  "t6#2024-10-26#publisher2",
                ],
                cursor: "t6#2024-10-26#publisher2",
              },
              GET_DAILY_STORAGE_BATCH_RESPONSE,
            ),
            "response",
          );
        }
        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t6#2024-10-26#publisher2",
          });

          // Verify
          assertThat(
            response,
            eqMessage(
              {
                rowKeys: [
                  "t6#2024-10-30#publisher1",
                  "t6#2024-10-31#publisher2",
                ],
                cursor: "t6#2024-10-31#publisher2",
              },
              GET_DAILY_STORAGE_BATCH_RESPONSE,
            ),
            "response 2",
          );
        }

        {
          // Execute
          let response = await handler.handle("", {
            cursor: "t6#2024-10-31#publisher2",
          });

          // Verify
          assertThat(
            response,
            eqMessage(
              {
                rowKeys: [],
              },
              GET_DAILY_STORAGE_BATCH_RESPONSE,
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
            key: "t6#2024-10-30#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t6#2024-10-31#publisher2",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "t6#2024-11-01#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
        ]);
        // Now is 2024-11-01T07:00:00Z
        let handler = new GetDailyStorageBatchHandler(
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
              rowKeys: ["t6#2024-10-30#publisher1"],
            },
            GET_DAILY_STORAGE_BATCH_RESPONSE,
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
