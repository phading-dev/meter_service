import { toDateISOString, toToday } from "../common/date_helper";
import { GetTodayWrtTimezoneHandlerInterface } from "@phading/product_meter_service_interface/web/handler";
import {
  GetTodayWrtTimezoneRequestBody,
  GetTodayWrtTimezoneResponse,
} from "@phading/product_meter_service_interface/web/interface";

export class GetTodayWrtTimezoneHandler extends GetTodayWrtTimezoneHandlerInterface {
  public static create(): GetTodayWrtTimezoneHandler {
    return new GetTodayWrtTimezoneHandler(() => new Date());
  }

  public constructor(private getDateNow: () => Date) {
    super();
  }

  public async handle(
    loggingPrefix: string,
    body: GetTodayWrtTimezoneRequestBody,
  ): Promise<GetTodayWrtTimezoneResponse> {
    return {
      date: toDateISOString(toToday(this.getDateNow())),
    };
  }
}
