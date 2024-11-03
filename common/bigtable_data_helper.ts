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
    value += data[columnFamily][columnName].value;
    data[columnFamily][columnName].value = value;
  }
  if (value >= Number.MAX_SAFE_INTEGER) {
    throw new Error(
      `FATAL! Column ${columnFamily}:${columnName} has exceeded MAX_SAFE_INTEGER!`,
    );
  }
}

// Normalizes data read from bigtable that comes with an array of values by taking only the first value.
export function normalizeData(data: any): any {
  for (let columnFamily in data) {
    for (let column in data[columnFamily]) {
      data[columnFamily][column] = {
        value: data[columnFamily][column][0].value,
      };
    }
  }
  return data;
}
