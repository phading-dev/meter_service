/*
Data takes the form of

{
  columnFamily: {
    column: {
      value: ...
    }
  }
}
*/

export function incrementColumn(
  data: any,
  columnFamily: string,
  columnName: string,
  value: number,
): void {
  if (!data[columnFamily]) {
    data[columnFamily] = {
      [columnName]: {
        value: value,
      },
    };
  } else if (!data[columnFamily][columnName]) {
    data[columnFamily][columnName] = {
      value: value,
    };
  } else {
    data[columnFamily][columnName].value += value;
  }
}

// Normalizes data read from bigtable that comes with an array of values by taking only the first value.
export function normalizeData(data: any): any {
  let dataToReturn: any = {};
  for (let columnFamily in data) {
    dataToReturn[columnFamily] = {};
    for (let column in data[columnFamily]) {
      dataToReturn[columnFamily][column] = {
        value: data[columnFamily][column][0].value,
      };
    }
  }
  return dataToReturn;
}
