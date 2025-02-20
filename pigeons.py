"""
Pigeons is a crap version of pandas.
 - I only made this because some clients don't let you install any packages so this will have to do.
 - You have some vague resemblence of a DataFrame
 - All fields are strings - so don't pass anything else in.
 - **The whole point is that you don't need any other package dependencies that aren't shipped with vanilla Python**
 - Just this one .py file

You would typically import using `import pigeons as cd`
You can then create a DataFrame like `dt = cd.DataFrame()`
You can import from a CSV: `dt = cd.from_csv('my_csv.csv')` - good chance it won't work.
You can do the following:
```
dt = dt1.merge(dt2, how='inner', left_on='blah', right_on='blah)
df.where("foo = 'yes'")
df.head(100)
df.to_csv('output_csv.csv')
data = df.fetch_all()
```

To load data without a csv, you should pass it in as a list of dictionaries, e.g. 
```
my_data = [{'foo': 'bar'},{'foo': 'baz'}]
dt = cd.DataFrame(data=my_data)
```

This is the same structure that `DataFrame.fetch_all()` will return results in. 

Good luck, you'll need it!

"""
import csv
import sqlite3
import os
import atexit

class HaveHitAWallError(Exception):
    """Custom Exception"""
    def __init__(self, message):
        super().__init__(message)  # Call Exception's constructor

    def __str__(self):
        return f"[Error] {self.args[0]}"  # Custom string representation


class DataFrameEngine:
    def __init__(self, db_path="pigeons.db"):
        self.db_path = db_path
        if os.path.exists(self.db_path):
            os.remove(db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._register_functions()

        atexit.register(self.cleanup)


    def _register_functions(self):
        def test(x):
            return 1
        
        self.conn.create_function("TEST", 1, test)
    
    def get_connection(self):
        return self.conn
    
    def cleanup(self):
        self.conn.close()
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
    
    def __del__(self):
        self.cleanup()

engine = DataFrameEngine()

class DataFrame:
    counter = 1
    def __init__(self, data=None, dtypes=None):
        self.engine = engine
        self.table_name = 'DataFrame_' + str(DataFrame.counter)
        
        if not dtypes and not data:
            dtypes = {'__pigeons': 'text'}
        elif not dtypes:
            dtypes = {}
            for f in list(data[0].keys()):
                dtypes[f] = 'text'

        self.dtypes = dtypes
        self.data = data
        self.view_sql = self._create_table()
        self.view_where = '1 = 1'
        self.index_slice = None

        DataFrame.counter += 1

        if self.data is not None:
            self._load_data()
        
    def __getitem__(self, index):        
        
        if isinstance(index, slice):
            start, stop, step = index.start, index.stop, index.step
            
            if start is None or start < 0: # not doing reverse slicing for now
                start = 0
            start_part = f"_idx >= {str(start)}"

            if stop is None:
                stop_part = ''
            
            elif stop < 0:
                stop_part = f" and _idx <= (select count(*){str(stop)} from ({self._return_df_sql()}))"
            
            elif stop > start:
                stop_part = f" and _idx < {str(stop)}"

            else:
                raise HaveHitAWallError("Whatever you were trying to do can't be done. Maybe stick to positive values for slices plz")


                
            view_sql = f"select * from vw_{self.table_name}"
            view_where = start_part + stop_part

            if step:
                raise HaveHitAWallError("can't do steps yet sorry")

        elif isinstance(index, list):
            cols = index
            view_sql = f"select {', '.join([c for c in cols])} from ({self._return_df_sql()})"
            view_where = '1=1'
            
        elif isinstance(index, int):
            view_sql = f"select * from ({self._return_df_sql()})"
            view_where = f"_idx = {index+1}"
        else:
            raise HaveHitAWallError("You didn't provide a slice of rows or a list of column names. What did you expect???")
        
        new_df = DataFrame()
        new_df._modify_view(view_sql=view_sql, view_where=view_where)
        return new_df

        
    def _get_create_sql(self):
        dtypes = ', '.join([f'{key} {value}' for key, value in self.dtypes.items()])
        table_sql = f"""
            CREATE TABLE {self.table_name} (
                    {dtypes}
            ) 
        """

        view_sql = f"""
            CREATE VIEW vw_{self.table_name} AS
            WITH _index 
            as
            (
                SELECT row_number() over () -1 as _idx, * FROM {self.table_name}
            )
            select * from _index
        """

        return table_sql, view_sql
    
    def _modify_view(self, view_sql, view_where):
        conn = self.engine.get_connection()
        cursor = conn.cursor()

        cursor.execute(f'DROP VIEW IF EXISTS vw_{self.table_name}')
        
        sql = f"""CREATE VIEW vw_{self.table_name} AS
        WITH _index
         as
          (
           select row_number() over () -1 as _idx , * from ({view_sql} WHERE {view_where})
            )
            select * from _index""" 
        cursor.execute(sql)
        self.view_sql= view_sql
        self.view_where = view_where
        
    def _return_df_sql(self):
        
        if self.index_slice:
            limit_stmt = f'{self.index_slice}'
        else:
            limit_stmt = '1=1'
        
        sql = f"""
            select * from vw_{self.table_name}
            where {limit_stmt} 
        """
        return sql
    

    def _create_table(self):
        conn = self.engine.get_connection()
        cursor = conn.cursor()
        table_sql, view_sql = self._get_create_sql()
        cursor.execute(table_sql)
        cursor.execute(view_sql)
        return view_sql
        
    
    def _insert_sql(self):
        sql = f"INSERT INTO {self.table_name} ({', '.join([x for x in self.dtypes])}) VALUES ({', '.join(['?' for x in self.dtypes])})"
        return sql
    
    def _load_data(self):
        conn = self.engine.get_connection()
        cursor = conn.cursor()
        
        for row in self.data:
            cursor.execute(self._insert_sql(), tuple(list(row.values())))
        
        conn.commit()


    def _limit_offset(self):
        pass


    def fetch_all(self):
        conn = self.engine.get_connection()
        cursor = conn.cursor()
        cursor.execute(self._return_df_sql())
        rows = cursor.fetchall()
        results = []
        for row in rows:
            results += [dict(row)]
    
        return results

    
    def merge(self, df, how, on=None, left_on=None, right_on=None):
        new_df = DataFrame()
        
        if on is None and (left_on is None or right_on is None):
            raise HaveHitAWallError("You did not set on, left_on, right_on attributes correctly. Do try again.")
        elif on is not None:
            left_on = on
            right_on = on
        elif (left_on is None or right_on is None):
            raise HaveHitAWallError("You did not set on, left_on, right_on attributes correctly. Do try again.")

        left_fields = list(self.dtypes.keys())
        right_fields = list(df.dtypes.keys())

        for f in left_fields:
            if f in right_fields:
                element = right_fields.index(f)
                if f != right_on:
                    
                    right_fields[element] = right_fields[element] + f' as {right_fields[element]}_r'
                else:
                    del right_fields[element]
        
        if right_fields == []:
            sep = ''
        else:
            sep = ', ' 
        join_type = {'inner': 'INNER JOIN', 'cross': 'CROSS JOIN', 'left': 'LEFT JOIN'}

        sql = f"""
        select {' ,'.join(['l.'+ x for x in left_fields])}
        {sep}{' ,'.join(['r.'+ x for x in right_fields])}
        from
            ({self._return_df_sql()}) l
        {join_type[how]}
            ({df._return_df_sql()}) r"""
        
        if how != 'cross':
            sql = sql + f" on l.{left_on} = r.{right_on}"

        new_df._modify_view(sql, '1=1')

        return new_df
    
    def where(self, view_where):
        self._modify_view(self.view_sql, view_where)
        return self
    
    def head(self, num_rows=10):
        conn = self.engine.get_connection()
        cursor = conn.cursor()
        cursor.execute(f"select * from ({self._return_df_sql()}) limit {str(num_rows)}")
        res = cursor.fetchall()
        results = []
        for index, row in enumerate(res):
            if index == 0:
                header = list(row.keys())
                max_len = {}
                for h in header:
                    max_len[h] = 1
            results += [dict(row)]

            for h in header:
                if max_len[h] < len(str(dict(row)[h])):
                    max_len[h] = len(str(dict(row)[h]))
        
        println = '|'.join([str(h).ljust(max_len[h]) for h in header])
        print(println)
        print(''.join('=' for x in println))

        for r in results:
            println = '|'.join([str(r[h]).ljust(max_len[h]) for h in header])
            print(println)

    def to_csv(self, file_path, sep=',', encoding='utf-8', include_header=True):
        results = self.fetch_all()

        with open(file_path, 'w', encoding=encoding) as file:
            for index, row in enumerate(results):
                if index == 0:
                    header = list(row.keys())
                    if include_header:
                        file.write(sep.join(header)+'\n')
                
                file.write(sep.join(['"' + str(row[h]) + '"' for h in header ])+'\n')
            
            file.close()

        

    

def from_csv(file_path, encoding='utf-8'):
    conn = engine.get_connection()
    cursor = conn.cursor()

    with open(file_path, 'r', encoding=encoding) as file:
        csv_reader = csv.reader(file)

        header = next(csv_reader)
        dtypes = {}
        for h in header:
            dtypes[h] = 'text'
        
        df = DataFrame(dtypes=dtypes)

        for row in csv_reader:
            cursor.execute(df._insert_sql(), (tuple(row)))

    conn.commit() 
    return df  
