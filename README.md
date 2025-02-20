# pigeons
Pigeons is a crap version of pandas
 - I only made this because some clients don't let you install any packages so this will have to do.
 - You have some vague resemblence of a DataFrame
 - All fields are strings - so don't pass anything else in.
 - **The whole point is that you don't need any other package dependencies that aren't shipped with vanilla Python**
 - Just this one .py file

You would typically import using `import pigeons as cd`
You can then create a DataFrame like `df = pg.DataFrame()`
You can import from a CSV: `df = pg.from_csv('my_csv.csv')` - good chance it won't work.
You can do the following:
```
df = df1.merge(df2, how='inner', left_on='blah', right_on='blah)
df.where("foo = 'yes'")
df.head(100)
df.to_csv('output_csv.csv')
data = df.fetch_all()
```

To load data without a csv, you should pass it in as a list of dictionaries, e.g. 
```
my_data = [{'foo': 'bar'},{'foo': 'baz'}]
df = pg.DataFrame(data=my_data)
```

This is the same structure that `DataFrame.fetch_all()` will return results in. 

Good luck, you'll need it!
