# Creating-a-Research-Database
This small, 21-table, easily-deployable database is designed to read in and store transcript, demographic, and transfer data on college students. Its most basic use case is to identify analytical cohorts of students with particular traits (e.g. first time in college) for further modeling and study. Other use cases include querying to produce basic descriptive results about different student populations.

In the python script, the phrase "frozen files" refers to sets of flat file extracts (.csv in this case) from a database, each of which represents the database at a particular moment in time. A concise database program like this can read in such extracts and begin knittting together a coherent picture of these education data over time.
