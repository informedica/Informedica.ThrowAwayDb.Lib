namespace Informedica.ThrowAwayDb.Lib


open System
open System.IO

open Microsoft.Data.SqlClient
open Informedica.Utils.Lib
open Informedica.Utils.Lib.BCL

open Utils


type Options =
    {
        DatabaseNamePrefix : string
        Collation : string
    }


type IThrowAwayDatabase  =
    abstract member ConnectionString : string
    abstract member Options : Options
    abstract member Created : bool
    abstract member Name : string
    abstract member Server : string
    abstract member OpenConnection : unit -> SqlConnection
    abstract member CreateSnapshot : unit -> unit
    abstract member RestoreSnapshot : unit -> unit
    abstract member ShapShot : string option
    inherit IDisposable



[<RequireQualifiedAccess>]
module ThrowAwayDatabase =


    let systemDbs = [ "master"; "tempdb"; "model"; "msdb" ]


    let defaultPrefix = "ThrowAwayDb"


    let getMasterConnectionString connString =
        connString
        |> SqlConnectionStringBuilder.tryCreate
        |> Option.map (fun bldr -> bldr.InitialCatalog <- "master"; bldr.ConnectionString)
        |> Option.get


    let private terminateConnections (db : IThrowAwayDatabase) =
        if db.Created |> not then ()
        else
            let connString =
                db.ConnectionString
                |> getMasterConnectionString
            $"ALTER DATABASE [{db.Name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE"
            |> SqlCommand.executeNonQuery connString
            |> ignore


    let dropDatabaseIfCreated (db : IThrowAwayDatabase) =
        if db.Created |> not then ()
        else
            db |> terminateConnections
            let connString = db.ConnectionString |> getMasterConnectionString
            match db.ShapShot with
            | None -> ()
            | Some ssn ->
                $"DROP DATABASE [{ssn}]"
                |> SqlCommand.executeNonQuery connString
                |> ignore
            $"DROP DATABASE [{db.Name}]"
            |> SqlCommand.executeNonQuery connString
            |> ignore


    let canPingDatabase connString =
        try
            let builder = SqlConnectionStringBuilder(connString)
            // make sure it doesn't take for ever
            builder.CommandTimeout <- 1
            builder.ConnectTimeout <- 1
            // try the connection string
            use conn = new SqlConnection(builder.ConnectionString)
            conn.Open()
            use cmd = new SqlCommand("SELECT GETDATE()", conn)
            cmd.ExecuteScalar() |> ignore
            true
        with
        | e ->
            $"Cannot ping database at: {connString}"
            |> Console.writeLineWarning
            $"With error:\n{e.Message}"
            |> Console.writeLineError
            false


    let databaseExists connString =
        let dbExists name  (reader: SqlDataReader) =
            // default database names to be case insensitive
            let name = name |> String.toLower
            let rec exists b acc =
                if b |> not || acc then acc
                else
                    let x =
                        reader.GetString(0)
                        |> String.toLower
                    x = name
                    |> exists (reader.Read())
            exists (reader.Read()) false

        connString
        |> SqlConnectionStringBuilder.tryCreate
        |> function
        | None        ->
            $"Couldn't create a connection string with {connString}"
            |> Console.writeLineWarning
            false
        | Some builder ->
            let dbName = builder.InitialCatalog
            builder.InitialCatalog <- "master"
            if builder.ConnectionString |> canPingDatabase |> not then false
            else
                if systemDbs |> List.exists (String.toLower >> ((=) (dbName |> String.toLower))) then
                    $"The database is a system database: {dbName}"
                    |> Console.writeLineWarning
                    true
                else
                    // make sure it doesn't wait forever
                    builder.ConnectTimeout <- 30
                    builder.CommandTimeout <- 30
                    // create the connection
                    use conn = new SqlConnection(builder.ConnectionString)
                    conn.Open()
                    // create the command
                    let inList = systemDbs |> List.map (sprintf "'%s'") |> String.concat ", "
                    let cmdText = $"SELECT NAME FROM sys.databases WHERE NAME NOT IN ({inList});"
                    use cmd = new SqlCommand(cmdText, conn)
                    // create the reader to check whether the database exists
                    use reader = cmd.ExecuteReader()
                    reader |> dbExists dbName


    let createDatabaseIfDoesNotExist (connString : string) (opts : Options) =
        connString
        |> SqlConnectionStringBuilder.tryCreate
        |> function
        | None ->
            $"Cannot create datebase with connection: {connString}"
            |> Console.writeLineWarning
            false
        | Some builder ->
            if builder.ConnectionString |> databaseExists then
                $"Database: {builder.InitialCatalog} already exists"
                |> Console.writeLineInfo
                true
            else
                let dbName = builder.InitialCatalog
                builder.InitialCatalog <- "master"

                let cmdText =
                    $"CREATE DATABASE {dbName}" +
                    (if opts.Collation |> String.isNullOrWhiteSpace then ""
                     else $" COLLATE {opts.Collation}")
                if SqlCommand.executeNonQuery builder.ConnectionString cmdText then
                    $"Created {dbName} on server {builder.DataSource}"
                    |> Console.writeLineInfo
                    true
                else
                    $"Create database {dbName} using command:\n{cmdText}\ndid not succeed"
                    |> Console.writeLineWarning
                    false


    let createSnapshot (db : IThrowAwayDatabase) =
        let ssName = $"{db.Name}_ss"
        let connString = db.ConnectionString |> getMasterConnectionString
        use conn = new SqlConnection(connString)
        conn.Open()
        let cmdText =
            "SELECT TOP 1 physical_name FROM sys.master_files WHERE name = 'master'"
        use cmd = new SqlCommand(cmdText, conn)
        let path = cmd.ExecuteScalar() :?> string
        if path |> String.isNullOrWhiteSpace then None
        else
            let fileName =
                path
                |> Path.getDirectoryName
                |> Path.combineWith $"{ssName}.ss"
            $"CREATE DATABASE [{ssName}] ON ( NAME = [{db.Name}], FILENAME = [{fileName}] ) AS SNAPSHOT OF [{db.Name}]"
            |> SqlCommand.executeNonQuery connString
            |> function
            | false -> None
            | true  ->
                $"Created snapshot: {ssName}"
                |> Console.writeLineInfo
                ssName |> Some


    let restoreSnapshot (db : IThrowAwayDatabase) =
        match db.ShapShot with
        | None ->
            $"There was no snapshot on: {db.Name}"
            |> Console.writeLineWarning
            ()
        | Some ssn ->
            let connString = db.ConnectionString |> getMasterConnectionString
            let exec s =
                SqlCommand.executeNonQuery connString s
                |> ignore

            db |> terminateConnections
            $"RESTORE DATABASE {db.Name} FROM DATABASE_SNAPSHOT = '{ssn}'"
            |> exec

            $"ALTER DATABASE [{db.Name}] SET MULTI_USER"
            |> exec

            $"Restored snapshot: {ssn}"
            |> Console.writeLineInfo


    let create connString opts =
        connString
        |> SqlConnectionStringBuilder.tryCreate
        |> function
        | None ->
            let msg =
                $"Couldn't create a database with connection string: {connString}"
            msg
            |> Console.writeLineError
            msg |> failwith
        | Some builder ->
            let dbPrefix =
                if opts.DatabaseNamePrefix |> String.isNullOrWhiteSpace then
                    defaultPrefix
                else
                    opts.DatabaseNamePrefix
            let guid = Guid.NewGuid().ToString("n").Substring(0, 10)
            builder.InitialCatalog <-
                $"{dbPrefix}{guid}"
            let created = createDatabaseIfDoesNotExist builder.ConnectionString opts
            let mutable ss = None

            {
                new IThrowAwayDatabase with
                    member this.ConnectionString = builder.ConnectionString
                    member this.Name = builder.InitialCatalog
                    member this.Server = builder.DataSource
                    member this.Options = opts
                    member this.Created = created
                    member this.CreateSnapshot () =
                        ss <- this |> createSnapshot
                    member this.RestoreSnapshot () =
                        this |> restoreSnapshot
                    member this.ShapShot = ss
                    member this.OpenConnection () =
                        let conn = new SqlConnection(this.ConnectionString)
                        conn.Open()
                        conn
                    member this.Dispose () =
                        dropDatabaseIfCreated this
                        $"Disposed {this.Name} on server {this.Server}"
                        |> Console.writeLineInfo
            }


    let emptyOpts =
        {
            DatabaseNamePrefix = ""
            Collation = ""
        }


    let fromDefaultLocalInstance () =
        let bldr = SqlConnectionStringBuilder.defaultBuilder ()
        create bldr.ConnectionString emptyOpts


    let fromLocalInstance server =
        let bldr = SqlConnectionStringBuilder.defaultBuilder ()
        bldr.DataSource <- server
        create bldr.ConnectionString emptyOpts


    let withSqlAuthorization userName passWord server =
        let bldr = SqlConnectionStringBuilder.defaultBuilder ()
        bldr.IntegratedSecurity <- false
        bldr.UserID <- userName
        bldr.Password <- passWord
        create bldr.ConnectionString emptyOpts
