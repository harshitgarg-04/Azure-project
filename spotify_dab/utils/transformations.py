class reusable:
    def dropColumn(self, df, column):
        df = df.drop(*column)
        return df