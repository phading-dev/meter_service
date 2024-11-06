import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { LoadPublishersToProcessMonthlyHandler } from "./load_publishers_to_process_monthly";
import { AccountType } from "@phading/user_service_interface/account_type";
import {
  LIST_ACCOUNTS,
  LIST_ACCOUNTS_REQUEST_BODY,
  ListAccountsResponse,
} from "@phading/user_service_interface/third_person/backend/interface";
import { eqMessage } from "@selfage/message/test_matcher";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import {
  assertReject,
  assertThat,
  containStr,
  eq,
} from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

let DEFAULT_PUBLISHER_DATA = {
  t: {
    w: {
      value: 0,
    },
    mb: {
      value: 0,
    },
  },
  c: {
    p: {
      value: "",
    },
  },
};

TEST_RUNNER.run({
  name: "LoadPublishersToProcessMonthlyTest",
  cases: [
    {
      name: "ColdStartForOneMonth",
      execute: async () => {
        // Prepare
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {
          accountIds: ["publisher1", "publisher2"],
        } as ListAccountsResponse;
        // 2024-11-01 10:xx:xx UTC
        let handler = new LoadPublishersToProcessMonthlyHandler(
          "2024-09",
          2,
          BIGTABLE,
          clientMock,
          () => new Date(1730499986000),
        );

        // Execute
        await handler.handle("", {});

        // Verify
        assertThat(clientMock.request.descriptor, eq(LIST_ACCOUNTS), "RC");
        assertThat(
          clientMock.request.body,
          eqMessage(
            {
              accountType: AccountType.PUBLISHER,
              limit: 2,
              createdTimeMsCursor: 1730448000000,
            },
            LIST_ACCOUNTS_REQUEST_BODY,
          ),
          "request body",
        );
        assertThat(
          (await BIGTABLE.row("l1").get())[0].data,
          eqData({
            c: {
              m: {
                value: "2024-10",
              },
              t: {
                value: 0,
              },
            },
          }),
          "cursor",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-10#publisher1").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "publisher1 month data",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-10#publisher2").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "publisher2 month loaded",
        );
      },
      tearDown: async () => {
        await Promise.all([BIGTABLE.deleteRows("l"), BIGTABLE.deleteRows("t")]);
      },
    },
    {
      name: "StartFromOctAndInterrupted_ResumeUntilNovDone",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "l1",
            data: {
              c: {
                m: {
                  value: "2024-10",
                },
                t: {
                  value: 0,
                },
              },
            },
          },
        ]);
        let clientMock = new NodeServiceClientMock();
        let times = 0;
        let requestCaptured: any;
        clientMock.send = async (request: any): Promise<any> => {
          times++;
          if (times === 1) {
            requestCaptured = request;
            return {
              accountIds: ["publisher1", "publisher2"],
              createdTimeMsCursor: 1234,
            } as ListAccountsResponse;
          }
          throw new Error("interrupt error");
        };
        // 2024-12-02 12:xx:xx UTC
        let handler = new LoadPublishersToProcessMonthlyHandler(
          "2024-09",
          2,
          BIGTABLE,
          clientMock,
          () => new Date(1733142386000),
        );

        // Execute
        let error = await assertReject(handler.handle("", {}));

        // Verify
        assertThat(error.message, containStr("interrupt error"), "error");
        assertThat(
          requestCaptured.body,
          eqMessage(
            {
              accountType: AccountType.PUBLISHER,
              limit: 2,
              createdTimeMsCursor: 1733040000000,
            },
            LIST_ACCOUNTS_REQUEST_BODY,
          ),
          "request body",
        );
        assertThat(
          (await BIGTABLE.row("l1").get())[0].data,
          eqData({
            c: {
              m: {
                value: "2024-11",
              },
              t: {
                value: 1234,
              },
            },
          }),
          "cursor",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-11#publisher1").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "publisher1 loaded",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-11#publisher2").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "publisher2 loaded",
        );

        // Prepare
        clientMock.send = async (request: any): Promise<any> => {
          requestCaptured = request;
          return {
            accountIds: ["publisher3"],
          } as ListAccountsResponse;
        };

        // Execute
        await handler.handle("", {});

        // Verify
        assertThat(
          requestCaptured.body,
          eqMessage(
            {
              accountType: AccountType.PUBLISHER,
              limit: 2,
              createdTimeMsCursor: 1234,
            },
            LIST_ACCOUNTS_REQUEST_BODY,
          ),
          "request body 2",
        );
        assertThat(
          (await BIGTABLE.row("l1").get())[0].data,
          eqData({
            c: {
              m: {
                value: "2024-11",
              },
              t: {
                value: 0,
              },
            },
          }),
          "cursor",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-11#publisher3").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "publisher3 loaded",
        );
      },
      tearDown: async () => {
        await Promise.all([BIGTABLE.deleteRows("l"), BIGTABLE.deleteRows("t")]);
      },
    },
    {
      name: "NothingLoaded",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "l1",
            data: {
              c: {
                m: {
                  value: "2024-10",
                },
                t: {
                  value: 0,
                },
              },
            },
          },
        ]);
        let clientMock = new NodeServiceClientMock();
        // 2024-11-02 23:xx:xx UTC
        let handler = new LoadPublishersToProcessMonthlyHandler(
          "2024-09",
          2,
          BIGTABLE,
          clientMock,
          () => new Date(1730591904000),
        );

        // Execute
        await handler.handle("", {});

        // Verify
        assertThat(clientMock.request, eq(undefined), "no request");
      },
      tearDown: async () => {
        await Promise.all([BIGTABLE.deleteRows("l"), BIGTABLE.deleteRows("t")]);
      },
    },
    {
      name: "LoadMultipleMonths",
      execute: async () => {
        // Prepare
        await BIGTABLE.insert([
          {
            key: "l1",
            data: {
              c: {
                m: {
                  value: "2024-11",
                },
                t: {
                  value: 0,
                },
              },
            },
          },
        ]);
        let clientMock = new (class extends NodeServiceClientMock {
          public async send(request: any): Promise<any> {
            if (request.body.createdTimeMsCursor === 1234) {
              return {
                accountIds: ["publisher3"],
              } as ListAccountsResponse;
            } else {
              return {
                accountIds: ["publisher1", "publisher2"],
                createdTimeMsCursor: 1234,
              } as ListAccountsResponse;
            }
          }
        })();
        // 2025-02-02 23:xx:xx UTC
        let handler = new LoadPublishersToProcessMonthlyHandler(
          "2024-09",
          2,
          BIGTABLE,
          clientMock,
          () => new Date(1738540704000),
        );

        // Execute
        await handler.handle("", {});

        // Verify
        assertThat(
          (await BIGTABLE.row("l1").get())[0].data,
          eqData({
            c: {
              m: {
                value: "2025-01",
              },
              t: {
                value: 0,
              },
            },
          }),
          "cursor",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-12#publisher1").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "2024-12#publisher1 loaded",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-12#publisher2").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "2024-12#publisher2 loaded",
        );
        assertThat(
          (await BIGTABLE.row("t7#2024-12#publisher3").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "2024-12#publisher3 loaded",
        );
        assertThat(
          (await BIGTABLE.row("t7#2025-01#publisher1").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "2025-01#publisher1 loaded",
        );
        assertThat(
          (await BIGTABLE.row("t7#2025-01#publisher2").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "2025-01#publisher2 loaded",
        );
        assertThat(
          (await BIGTABLE.row("t7#2025-01#publisher3").get())[0].data,
          eqData(DEFAULT_PUBLISHER_DATA),
          "2025-01#publisher3 loaded",
        );
      },
      tearDown: async () => {
        await Promise.all([BIGTABLE.deleteRows("l"), BIGTABLE.deleteRows("t")]);
      },
    },
  ],
});
