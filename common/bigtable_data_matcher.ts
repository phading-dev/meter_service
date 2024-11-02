import { MatchFn, assert, assertThat, eq } from "@selfage/test_matcher";

/*
`expected` takes the form of
{
  columnFamily: {
    column: {
      value: ...
    }
  }
}

Only matches the first cell from each column in the actual data.
*/
export function eqData(expected: any): MatchFn<any> {
  return (actual) => {
    for (let columnFamily in expected) {
      assert(actual[columnFamily], `${columnFamily} to exist`, "not");
      for (let column in expected[columnFamily]) {
        assert(
          actual[columnFamily][column],
          `${columnFamily}:${column} to exist`,
          "not",
        );
        assertThat(
          actual[columnFamily][column][0].value,
          eq(expected[columnFamily][column].value),
          `${columnFamily}:${column}`,
        );
      }
      assertThat(
        Object.keys(actual[columnFamily]).length,
        eq(Object.keys(expected[columnFamily]).length),
        `number of columns in the family ${columnFamily}`,
      );
    }
    assertThat(
      Object.keys(actual).length,
      eq(Object.keys(expected).length),
      `number of column families`,
    );
  };
}
