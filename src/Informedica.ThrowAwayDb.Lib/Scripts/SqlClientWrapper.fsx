
#r "nuget: Microsoft.Data.SqlClient"

open Microsoft.Data.SqlClient
open System.Data

[<RequireQualifiedAccess>]
module SqlConnectionStringBuilder =

    let tryCreate connString =
        try
            SqlConnectionStringBuilder(connString)
            |> Some
        with
        | e ->
            printfn $"cannot create the connection string builder:\n{e.Message}"
            None

[<RequireQualifiedAccessAttribute>]
module SqlCommand =

    let executeNonQuery connString cmdText =
        try
            use conn = new SqlConnection(connString)
            use cmd = new SqlCommand(cmdText, conn)
            conn.Open()
            cmd.ExecuteNonQuery() |> ignore
            true
        with
        | e ->
            $"Could not execute: {cmdText}\nWith exception:\n{e.Message}"
            |> printfn "%s"
            false

