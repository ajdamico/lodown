get_catalog_sia <-
  function( data_name = "sia" , output_dir , ... ){

    catalog <- NULL

    # Part 1: specific tables
    for( sia_portal in paste0( "ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/" , c( "199407_200712/", "200801_/") , "Dados/" ) ) {

      filenames <- RCurl::getURL( sia_portal , verbose=FALSE , ftp.use.epsv=FALSE , dirlistonly = TRUE , crlf = FALSE )
      filenames <- strsplit( filenames, "\r*\n")[[1]]
      filenames <- filenames[ grepl( ".dbc$" , filenames ) ]

      full_url = paste( sia_portal, filenames, sep = "")

      year_lines <- substr( gsub( "[^0-9]" , "" , basename( filenames ) ) , 1 , 2 )
      time_thresh <- year_lines < 94
      year_lines[ time_thresh ] <- paste0( "20" , year_lines[ time_thresh ] )
      year_lines[ !time_thresh ] <- paste0( "19" , year_lines[ !time_thresh ] )

      mnth_lines <- substr( gsub( "[^0-9]" , "" , basename( filenames ) ) , 3 , 4 )

      uf <- toupper( substr( basename( filenames ) , nchar( basename( filenames ) ) - 9, nchar( basename( filenames ) ) - 8 ) )
      tab.type <- toupper( substr( basename( filenames ) , 1 , nchar( basename( filenames ) ) - 10 ) )

      catalog <-
        rbind( catalog ,
               data.frame(
                 type =  tab.type ,
                 uf = uf ,
                 year = year_lines ,
                 month = mnth_lines ,
                 full_url = full_url ,
                 db_tablename = paste0( tab.type , year_lines ) ,
                 dbfolder = paste0( output_dir , "/MonetDB" ) ,
                 output_filename = paste( output_dir , gsub( "\\.dbc" , ".rda" , tolower( basename( full_url ) ) ) , sep = "/" ) ,
                 stringsAsFactors = FALSE
               )
        )

    }

    catalog <- catalog[ with( catalog , order( type , year , uf , month ) ) , ]

    catalog

  }


lodown_sia <-
  function( data_name = "sia" , catalog , doc_dir = NULL , ... ){

    if ( !requireNamespace( "read.dbc" , quietly = TRUE ) ) stop( "read.dbc needed for this function to work. to install it, type `install.packages( 'read.dbc' )`" , call. = FALSE )

    tf <- tempfile()

    for ( i in seq_len( nrow( catalog ) ) ){

      # download the file
      cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

      x <- read.dbc::read.dbc( tf )

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

      # if this is the final catalog entry for the unique db_tablename, then write them all to the database
      if( i == max( which( catalog$db_tablename == catalog[ i , 'db_tablename' ] ) ) ){

        correct_columns <- get( catalog[ i , 'db_tablename' ] )

        # open the connection to the monetdblite database
        db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

        # loop through all tables that match the current db_tablename
        for( this_file in catalog[ catalog[ i , 'db_tablename' ] == catalog$db_tablename , 'output_filename' ] ){

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

        }

        # disconnect from the current monet database
        DBI::dbDisconnect( db , shutdown = TRUE )

      }


      # delete the temporary files
      suppressWarnings( file.remove( tf ) )

      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

    }

    # get documentation
    if ( !is.null( doc_dir ) ) {
      for( sia_portal in paste0( "ftp://ftp.datasus.gov.br/dissemin/publicos/SIASUS/" , c( "199407_200712/", "200801_/") ) ) {

        # get directories under sia_portal url
        dir.names <- RCurl::getURL( sia_portal , ftp.use.epsv = FALSE , dirlistonly = TRUE )
        dir.names <- strsplit( dir.names, "\r*\n")[[1]]

        # figure out documentation folders
        dir.names <- grep( "^Dados" , dir.names , value = TRUE , invert = TRUE , ignore.case = TRUE )

        for ( directory in dir.names ) {

          filenames <- RCurl::getURL( paste0( sia_portal , directory , sep = "/" ), ftp.use.epsv = FALSE , dirlistonly = TRUE )
          filenames <- strsplit( filenames, "\r*\n")[[1]]

          for ( docfile in filenames ) {

            docurl <- paste0( sia_portal , directory , "/" , docfile  )

            # determine the document directory
            pth <- paste0( doc_dir , "/" , if( grepl( "200801_" , sia_portal ) ) paste0( "Documentation/" , "200801_" ) else paste0( "Documentation/" , "199407_200712" ) , "/" , directory , "/" )

            # if the directory doesn't exist, creates
            if ( !dir.exists(pth) ){
              dir.create( pth , recursive = TRUE , showWarnings = FALSE )
            }

            cachaca( this_url = docurl , destfile = paste0( pth , "/" , docfile ) , mode = "wb" , attempts = 20 )
          }

        }

      }

    }

    invisible( TRUE )

  }

