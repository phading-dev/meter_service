import "../../../local/env";
import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { ProcessMonthlyMeterReadingHandler } from "./process_monthly_meter_reading_handler";
import {
  GENERATE_TRANSACTION_STATEMENT,
  GENERATE_TRANSACTION_STATEMENT_REQUEST_BODY,
} from "@phading/commerce_service_interface/node/interface";
import { ProductID } from "@phading/price";
import { AmountType } from "@phading/price/amount_type";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "ProcessMonthlyMeterReadingHandlerTest",
  cases: [
    {
      name: "Process",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#01",
            data: {
              t: {
                ws: {
                  value: 12,
                },
                nk: {
                  value: 4200,
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#20",
            data: {
              t: {
                uk: {
                  value: 8000,
                },
                smm: {
                  value: 56000,
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#30",
            data: {
              t: {
                ws: {
                  value: 1300,
                },
                nk: {
                  value: 1400,
                },
                uk: {
                  value: 88000,
                },
                smm: {
                  value: 96000,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            switch (request.descriptor) {
              case GENERATE_TRANSACTION_STATEMENT:
                this.request = request;
                break;
              default:
                throw new Error(`Unexpected.`);
            }
          }
        })();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t5#2024-10#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f4#publisher1#2024-10").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 1312,
              },
              nm: {
                value: 7,
              },
              um: {
                value: 94,
              },
              smh: {
                value: 2534,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "publisher1",
              month: "2024-10",
              positiveAmountType: AmountType.CREDIT,
              lineItems: [
                {
                  productID: ProductID.NETWORK,
                  quantity: 7,
                },
                {
                  productID: ProductID.UPLOAD,
                  quantity: 94,
                },
                {
                  productID: ProductID.STORAGE,
                  quantity: 2534,
                },
                {
                  productID: ProductID.SHOW_CREDIT,
                  quantity: 1312,
                },
              ],
            },
            GENERATE_TRANSACTION_STATEMENT_REQUEST_BODY,
          ),
          "generate transaction statement request",
        );
        assertThat(
          (await BIGTABLE.row("t5#2024-10#publisher1").exists())[0],
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
      name: "OnlyUploadAndStorage",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#01",
            data: {
              t: {
                uk: {
                  value: 8000,
                },
                smm: {
                  value: 56000,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            switch (request.descriptor) {
              case GENERATE_TRANSACTION_STATEMENT:
                this.request = request;
                break;
              default:
                throw new Error(`Unexpected.`);
            }
          }
        })();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t5#2024-10#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f4#publisher1#2024-10").get())[0].data,
          eqData({
            t: {
              um: {
                value: 8,
              },
              smh: {
                value: 934,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "publisher1",
              month: "2024-10",
              positiveAmountType: AmountType.CREDIT,
              lineItems: [
                {
                  productID: ProductID.NETWORK,
                  quantity: 0,
                },
                {
                  productID: ProductID.UPLOAD,
                  quantity: 8,
                },
                {
                  productID: ProductID.STORAGE,
                  quantity: 934,
                },
                {
                  productID: ProductID.SHOW_CREDIT,
                  quantity: 0,
                },
              ],
            },
            GENERATE_TRANSACTION_STATEMENT_REQUEST_BODY,
          ),
          "generate transaction statement request",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("t");
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("f");
      },
    },
    {
      name: "OnlyWatchTimeAndNetwork",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "t5#2024-10#publisher1",
            data: {
              c: {
                p: {
                  value: "",
                },
              },
            },
          },
          {
            key: "d5#2024-10#publisher1#01",
            data: {
              t: {
                ws: {
                  value: 12,
                },
                nk: {
                  value: 4200,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            switch (request.descriptor) {
              case GENERATE_TRANSACTION_STATEMENT:
                this.request = request;
                break;
              default:
                throw new Error(`Unexpected.`);
            }
          }
        })();
        let handler = new ProcessMonthlyMeterReadingHandler(
          BIGTABLE,
          clientMock,
        );

        // Execute
        await handler.handle("", {
          rowKey: "t5#2024-10#publisher1",
        });

        // Verify
        assertThat(
          (await BIGTABLE.row("f4#publisher1#2024-10").get())[0].data,
          eqData({
            t: {
              ws: {
                value: 12,
              },
              nm: {
                value: 5,
              },
            },
          }),
          "final publisher data",
        );
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountId: "publisher1",
              month: "2024-10",
              positiveAmountType: AmountType.CREDIT,
              lineItems: [
                {
                  productID: ProductID.NETWORK,
                  quantity: 5,
                },
                {
                  productID: ProductID.UPLOAD,
                  quantity: 0,
                },
                {
                  productID: ProductID.STORAGE,
                  quantity: 0,
                },
                {
                  productID: ProductID.SHOW_CREDIT,
                  quantity: 12,
                },
              ],
            },
            GENERATE_TRANSACTION_STATEMENT_REQUEST_BODY,
          ),
          "generate transaction statement request",
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
