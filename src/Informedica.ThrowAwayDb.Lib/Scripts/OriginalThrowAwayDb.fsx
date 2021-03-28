
#r "nuget: ThrowAwayDb"

open ThrowawayDb

let connString =
    @"Data Source=VOXDB-PICURED01;Initial Catalog=master;Persist Security Info=True;Integrated Security=SSPI;"

do
    use db = ThrowawayDatabase.Create(connString)
    printfn $"connection string: {db.ConnectionString}"
