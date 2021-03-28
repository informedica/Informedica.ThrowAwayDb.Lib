namespace Informedica.ThrowAwayDb.Lib


module Utils =

    
    open System
    open System.IO
    
    open Microsoft.Data.SqlClient
    open Informedica.Utils.Lib
    open Informedica.Utils.Lib.BCL
    
    
    
    [<RequireQualifiedAccessAttribute>]
    module Path =
    
        let getDirectoryName (path : string) = Path.GetDirectoryName(path)
    
    
    [<RequireQualifiedAccessAttribute>]
    module Console =
    
    
        type MessageType = | Info | Warning | Error
    
        let writeLine mt (s : string) =
            Console.ForegroundColor <-
                match mt with
                | Info -> ConsoleColor.Blue
                | Warning -> ConsoleColor.Yellow
                | Error -> ConsoleColor.Red
            Console.WriteLine(s)
            Console.ForegroundColor <- ConsoleColor.White
    
        let writeLineInfo = writeLine Info
        let writeLineWarning = writeLine Warning
        let writeLineError = writeLine Error
    
    
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
    
    
        let defaultBuilder () =
            SqlConnectionStringBuilder(@"Data Source=.;Initial Catalog=master;Persist Security Info=True;Integrated Security=SSPI;")
    
    
    
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
    

