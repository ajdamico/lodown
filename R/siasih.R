get_catalog_siasih <-
  function( data_name = "siasih" , output_dir , ... ){

    output_dir <- gsub( "\\\\", "/", output_dir )

    sia_path <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/"
    sih_path <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/"
    cnes_path <- "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/"

    sia_files <- recursive_ftp_scrape( sia_path )
    sih_files <- recursive_ftp_scrape( sih_path )
    cnes_files <- recursive_ftp_scrape( cnes_path )

    catalog <-
      data.frame(
        full_url = c( sia_files , sih_files , cnes_files ) ,
        stringsAsFactors = FALSE
      )

    catalog$type <-
      ifelse( grepl( cnes_path , catalog$full_url ) , "cnes" ,
              ifelse( grepl( sia_path , catalog$full_url ) , "sih" ,
                      ifelse( grepl( sih_path , catalog$full_url ) , "sia" , NA ) ) )

    catalog$output_filename <-
      gsub( "dados/" , "" ,
            gsub( "\\.(dbc$|dbf$)" , "\\.rds" ,
                  gsub( "ftp://ftp.datasus.gov.br/dissemin/publicos/" , paste0( output_dir , "/" ) ,
                        tolower( catalog$full_url ) ) , ignore.case = TRUE ) )

    year_lines <- gsub( "[^0-9]" , "" , basename( catalog$full_url ) )
    #year_lines <- substr( gsub( "[^0-9]" , "" , basename( catalog$full_url ) ) , 1 , 2 )

    catalog$year <-
      ifelse( nchar( year_lines ) == 4 & as.numeric( year_lines ) >= 9000 , 1900 + as.numeric( substr( year_lines , 1 , 2 ) ) ,
              ifelse( nchar( year_lines ) == 4 & as.numeric( year_lines ) < 9000 , 2000 + as.numeric( substr( year_lines , 1 , 2 ) ) , NA ) )

    get_last <- function ( str , n = 1 ) substr(str,(nchar(str)+1)-n,nchar(str))
    catalog$db_tablename <-
      ifelse ( grepl( "\\.dbc$" , catalog$full_url ) ,
               paste0(
                 ifelse( grepl( "/cnes" , catalog$full_url , ignore.case = TRUE ) , "cnes" ,
                         ifelse( grepl( "/sihsus" , catalog$full_url , ignore.case = TRUE ) , "sih" ,
                                 ifelse( grepl( "/siasus" , catalog$full_url , ignore.case = TRUE ) , "sia" , NA ) ) ) , "_" ,
                 gsub( "[a-zA-Z]{2}[0-9]{4}.*", "" , tolower( basename( catalog$full_url ) ) ) ,
                 ifelse( grepl( "CNES", catalog$full_url , ignore.case = TRUE ), "", paste0( "_" , gsub( ".*(SUS/|CNES/)|/Dados.*", "" , dirname(catalog$full_url) ) ) ) ), NA )


    catalog$dbfolder <- ifelse( is.na( catalog$db_tablename ) , NA , paste0( output_dir , "/MonetDB" ) )

    catalog[ !grepl( "\\.(exe$|xml$|csv$|dbf$)" , catalog$full_url , ignore.case = TRUE ) , ]



  }


lodown_siasih <-
  function( data_name = "siasih" , catalog , ... ){

    if ( !requireNamespace( "read.dbc" , quietly = TRUE ) ) stop( "read.dbc needed for this function to work. to install it, type `install.packages( 'read.dbc' )`" , call. = FALSE )

    tf <- tempfile()

    for ( i in seq_len( nrow( catalog ) ) ){

      # download the file
      cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , attempts = 10 )

      if ( !grepl( "\\.dbc$" , basename( catalog[ i , "full_url" ] ) , ignore.case = TRUE ) ) {

        file.copy( tf , catalog[ i , 'output_filename' ] )

      } else {

        x <- read.dbc::read.dbc( tf )

        # remove trailing spaces
        names( x ) <- trimws( names( x ) , which = "both" )

        # convert all column names to lowercase
        names( x ) <- tolower( names( x ) )

        # add underscores after monetdb illegal names
        for ( j in names( x )[ toupper( names( x ) ) %in% getFromNamespace( "reserved_monetdb_keywords" , "MonetDBLite" ) ] ) names( x )[ names( x ) == j ] <- paste0( j , "_" )

        # coerce factor columns to character
        x[ sapply( x , class ) == "factor" ] <- sapply( x[ sapply( x , class ) == "factor" ] , as.character )

        # figure out which columns really ought to be numeric
        for( this_col in names( x ) ){

          # if the column can be coerced without a warning, coerce it to numeric
          this_result <- tryCatch( as.numeric( x[ , this_col ] ) , warning = function(c) NULL )

          if( !is.null( this_result ) ) x[ , this_col ] <- as.numeric( x[ , this_col ] )

        }

        catalog[ i , 'case_count' ] <- nrow( x )

        saveRDS( x , file = catalog[ i , 'output_filename' ] )

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

        # print process tracker
        cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

        # if this is the final catalog entry for the unique db_tablename, then write them all to the database
        if( i == max( which( catalog$db_tablename == catalog[ i , 'db_tablename' ] ) ) ){

          correct_columns <- get( catalog[ i , 'db_tablename' ] )

          # open the connection to the monetdblite database
          db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

          # loop through all tables that match the current db_tablename
          for( this_file in catalog[ ( catalog[ i , 'db_tablename' ] == catalog$db_tablename ) & ! is.na( catalog$db_tablename ) , 'output_filename' ] ){

            x <- readRDS( this_file )

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
            saveRDS( x , file = this_file )

            # append the file to the database
            DBI::dbWriteTable( db , catalog[ i , 'db_tablename' ] , x , append = TRUE , row.names = FALSE )

            file_index <- seq_along( catalog[ ( catalog[ i , 'db_tablename' ] == catalog$db_tablename ) & ! is.na( catalog$db_tablename ) , 'output_filename' ] ) [ this_file == catalog[ ( catalog[ i , 'db_tablename' ] == catalog$db_tablename ) & ! is.na( catalog$db_tablename ) , 'output_filename' ] ]
            cat( paste0( data_name , " entry " , file_index , " of " , nrow( catalog[ catalog$db_tablename == catalog[ i , 'db_tablename' ] , ] ) , " stored at '" , catalog[ i , 'db_tablename' ] , "'\r" ) )

          }

          # disconnect from the current monet database
          DBI::dbDisconnect( db , shutdown = TRUE )

        }

      }


      # delete the temporary files
      suppressWarnings( file.remove( tf ) )

    }

  }

