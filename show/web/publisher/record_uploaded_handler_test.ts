import { BIGTABLE } from "../../../common/bigtable";
import { eqData } from "../../../common/bigtable_data_matcher";
import { RecordUploadedHandler } from "./record_uploaded_handler";
import { ExchangeSessionAndCheckCapabilityResponse } from "@phading/user_session_service_interface/node/interface";
import { NodeServiceClientMock } from "@selfage/node_service_client/client_mock";
import { assertThat, eq } from "@selfage/test_matcher";
import { TEST_RUNNER } from "@selfage/test_runner";

TEST_RUNNER.run({
  name: "RecordUploadedHandlerTest",
  cases: [
    {
      name: "Success",
      execute: async () => {
        // Prepare
        let clientMock = new NodeServiceClientMock();
        clientMock.response = {
          accountId: "publisher1",
          canPublishShows: true,
        } as ExchangeSessionAndCheckCapabilityResponse;
        // 2024-11-26T11:00:00Z
        let handler = new RecordUploadedHandler(
          BIGTABLE,
          clientMock,
          () => new Date(1732618800000),
        );

        // Execute
        await handler.handle(
          "",
          {
            name: "newVideoFile",
            uploadedBytes: 1100,
          },
          "sessionStr",
        );

        // Verify
        assertThat(
          (await BIGTABLE.row("t6#2024-11-26#publisher1").exists())[0],
          eq(true),
          "task added",
        );
        assertThat(
          (await BIGTABLE.row("d6#2024-11-26#publisher1").get())[0].data,
          eqData({
            u: {
              newVideoFile: {
                value: 1100,
              },
            },
          }),
          "set",
        );
      },
      tearDown: async () => {
        await BIGTABLE.deleteRows("d");
        await BIGTABLE.deleteRows("t");
      },
    },
  ],
});
