import { TIMEZONE_OFFSET } from "../common/params";
import { GetTimezoneOffsetHandlerInterface } from "@phading/product_meter_service_interface/web/handler";
import {
  GetTimezoneOffsetRequestBody,
  GetTimezoneOffsetResponse,
} from "@phading/product_meter_service_interface/web/interface";

export class GetTimezoneOffsetHandler extends GetTimezoneOffsetHandlerInterface {
  public static create(): GetTimezoneOffsetHandler {
    return new GetTimezoneOffsetHandler();
  }

  public constructor() {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: GetTimezoneOffsetRequestBody,
  ): Promise<GetTimezoneOffsetResponse> {
    return {
      negativeOffset: TIMEZONE_OFFSET,
    };
  }
}
