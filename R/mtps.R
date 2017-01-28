get_catalog_mtps <-
  function( data_name = "mtps" , output_dir , ... ){

    output_dir <- gsub( "\\\\", "/", output_dir )

    url_path <- "ftp://ftp.mtps.gov.br/pdet/microdados/"

    mtps_files <- lodown:::recursive_ftp_scrape( url_path )

    catalog <-
      data.frame(
        full_url = mtps_files ,
        stringsAsFactors = FALSE
      )

    catalog$type <-
      ifelse( grepl( "layout|xls$|pdf$" , catalog$full_url , ignore.case = TRUE ) , "docs" ,
              ifelse( grepl( "RAIS" , catalog$full_url ) , "rais" ,
                      ifelse( grepl( "CAGED" , catalog$full_url ) , "caged" , NA ) )
      )

    catalog$subtype[ catalog$type == "caged" ] <- NA
    catalog$subtype[ catalog$type == "rais" ] <-
      ifelse( grepl( "estb" , catalog$full_url[ catalog$type == "rais" ] , ignore.case = TRUE ) , "estabelecimento" , "vinculo" )

    catalog$output_filename <- gsub( "7z$|zip$" , "rda" ,
                                                gsub( "ftp://ftp.mtps.gov.br/pdet/microdados/" , paste0( output_dir , "/" ) , tolower( catalog$full_url ) ) ,
                                                ignore.case = TRUE )
    catalog$output_filename <- sapply( catalog$output_filename , URLdecode )

    year_lines <- NULL
    year_lines [ catalog$type == "rais" & !is.na( catalog$type ) ] <-
      gsub( "[^0-9]" , "" ,
            gsub ( "\\..*" , "\\2" , basename( catalog$full_url ) [ grepl( ".7z$|.zip$" , catalog$full_url , ignore.case = TRUE ) ] )
      )[ catalog$type == "rais" & !is.na( catalog$type ) ]
    year_lines [ catalog$type == "caged" & !is.na( catalog$type ) ] <-
      gsub( "[^0-9]" , "" ,
            gsub ( "\\..*" , "\\2" , basename( catalog$full_url ) [ grepl( ".7z$|.zip$" , catalog$full_url , ignore.case = TRUE ) ] )
      )[ catalog$type == "caged" & !is.na( catalog$type ) ]
    get_last <- function ( str , n = 1 ) substr(str,(nchar(str)+1)-n,nchar(str))
    year_lines [ catalog$type == "caged" & !is.na( catalog$type ) ] <-
      get_last( year_lines [ catalog$type == "caged" & !is.na( catalog$type ) ] , 4 )
    catalog$year <- year_lines

    month_lines <- NULL
    month_lines [ catalog$type == "caged" & !is.na( catalog$type ) ] <-
      gsub( "[^0-9]" , "" ,
            gsub ( "\\..*" , "\\2" , basename( catalog$full_url ) [ grepl( ".7z$|.zip$" , catalog$full_url , ignore.case = TRUE ) ] )
      )[ catalog$type == "caged" & !is.na( catalog$type ) ]
    month_lines [ nchar( month_lines ) > 4 & !is.na( month_lines ) ] <-
      substr( month_lines [ nchar( month_lines ) > 4 & !is.na( month_lines ) ] , 1 , 2 )
    month_lines [ nchar( month_lines ) > 2 & !is.na( month_lines ) ] <- NA
    catalog$month <- month_lines

    catalog$db_tablename <-
      ifelse( !is.na( catalog[ , c( "year" ) ] ) ,
              paste0(
                ifelse( catalog$type != "docs" , catalog$type , "" ) ,
                ifelse( !is.na( catalog$subtype ) , paste0( "_", catalog$subtype ) , "" ) ,
                ifelse( !is.na( catalog$year ) & catalog$type != "caged" , paste0( "_", catalog$year ) , "" ) ) , NA )

    catalog$dbfolder <- ifelse( is.na( catalog$db_tablename ) , NA , paste0( output_dir , "/MonetDB" ) )

    catalog

  }



lodown_mtps <-
  function( data_name = "mtps" , catalog , path_to_7z = if( .Platform$OS.type != 'windows' ) '7za' else normalizePath( "C:/Program Files/7-zip/7z.exe" ) , ... ){

    tf <- tempfile()

    for ( i in seq_len( nrow( catalog ) ) ){


      # download the file
      cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

      if( !grepl( ".7z$|.zip$" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ){

        file.copy( tf , catalog[ i , 'output_filename' ] )

      } else {

        # build the string to send to DOS
        dos.command <- paste0( '"' , normalizePath( path_to_7z ) , '" x "' , normalizePath( tf ) , '" -o"' , paste0( tempdir() , "\\unzipped" ) , '"' )

        system( dos.command , show.output.on.console = FALSE )

        this_data_file <- list.files( paste0( tempdir() , "\\unzipped" ) , full.names = TRUE )

        this_data_file <- grep( "\\.csv|\\.txt$", this_data_file, value = TRUE, ignore.case = TRUE )

        x <- read.csv2( this_data_file , sep = ";" , dec = "," , header = TRUE )

        suppressWarnings( unlink( paste0( tempdir() , "\\unzipped" ) , recursive = TRUE ) )

        # convert all column names to lowercase
        names( x ) <- tolower( names( x ) )

        # add underscores after monetdb illegal names
        for ( j in names( x )[ toupper( names( x ) ) %in% getFromNamespace( "reserved_monetdb_keywords" , "MonetDBLite" ) ] ) names( x )[ names( x ) == j ] <- paste0( j , "_" )

        # remove trailing spaces
        names( x ) <- trimws( names( x ) , which = "both" )

        # coerce factor columns to character
        x[ sapply( x , class ) == "factor" ] <- sapply( x[ sapply( x , class ) == "factor" ] , as.character )

        # figure out which columns really ought to be numeric
        for( this_col in names( x ) ){

          # if the column can be coerced without a warning, coerce it to numeric
          this_result <- tryCatch( as.numeric( x[ , this_col ] ) , warning = function(c) NULL )

          if( !is.null( this_result ) ) x[ , this_col ] <- as.numeric( x[ , this_col ] )

        }

        catalog[ i , 'case_count' ] <- nrow( x )

        save( x , file = catalog[ i , 'output_filename' ] )

        these_cols <- sapply( x , class )

        these_cols <- data.frame( col_name = names( these_cols ) , col_type = these_cols , stringsAsFactors = FALSE )

        if( exists( catalog[ i , 'db_tablename' ] ) ){

          same_table_cols <- get( catalog[ i , 'db_tablename' ] )
          same_table_cols <- unique( rbind( these_cols , same_table_cols ) )

        } else same_table_cols <- these_cols

        dupe_cols <- same_table_cols$col_name[ duplicated( same_table_cols$col_name ) ]

        # if there's a duplicate, remove the numeric typed column
        same_table_cols <- same_table_cols[ !( same_table_cols$col_type == 'numeric' & same_table_cols$col_name %in% dupe_cols ) , ]

        assign( catalog[ i , 'db_tablename' ] , same_table_cols )

        # process tracker
        cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

        # if this is the final catalog entry for the unique db_tablename, then write them all to the database
        if( i == max( which( catalog$db_tablename == catalog[ i , 'db_tablename' ] ) ) ){

          correct_columns <- get( catalog[ i , 'db_tablename' ] )

          # open the connection to the monetdblite database
          db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

          # loop through all tables that match the current db_tablename
          for( this_file in catalog[ catalog$db_tablename %in% catalog[ i , 'db_tablename' ] , 'output_filename' ] ){

            load( this_file )

            for( this_col in setdiff( correct_columns$col_name , names( x ) ) ) x[ , this_col ] <- NA

            # get final table types
            same_table_cols <- get( catalog[ i , 'db_tablename' ] )

            for( this_row in seq( nrow( same_table_cols ) ) ){

              if( same_table_cols[ this_row , 'col_type' ] != class( x[ , same_table_cols[ this_row , 'col_name' ] ] ) ){

                if( same_table_cols[ this_row , 'col_type' ] == 'numeric' ) x[ , same_table_cols[ this_row , 'col_name' ] ] <- as.numeric( x[ , same_table_cols[ this_row , 'col_name' ] ] )
                if( same_table_cols[ this_row , 'col_type' ] == 'character' ) x[ , same_table_cols[ this_row , 'col_name' ] ] <- as.character( x[ , same_table_cols[ this_row , 'col_name' ] ] )

              }

            }

            # put the columns of x in alphabetical order so they're always the same
            x <- x[ sort( names( x ) ) ]

            # re-save the file
            save( x , file = this_file )

            # append the file to the database
            DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , x , append = TRUE , row.names = FALSE )

            file_index <- seq_along( catalog[ ( catalog[ i , 'db_tablename' ] == catalog$db_tablename ) & ! is.na( catalog$db_tablename ) , 'output_filename' ] ) [ this_file == catalog[ ( catalog[ i , 'db_tablename' ] == catalog$db_tablename ) & ! is.na( catalog$db_tablename ) , 'output_filename' ] ]
            cat( paste0( data_name , " entry " , file_index , " of " , nrow( catalog[ catalog$db_tablename == catalog[ i , 'db_tablename' ] , ] ) , " stored at '" , catalog[ i , 'db_tablename' ] , "'\r\n" ) )

          }

          # disconnect from the current monet database
          DBI::dbDisconnect( db , shutdown = TRUE )

        }


      }


      # delete the temporary files
      suppressWarnings( file.remove( tf ) )

    }

    catalog

  }

