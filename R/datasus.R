get_catalog_datasus <-
	function( data_name = "datasus" , output_dir , ... ){

		sim_path <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/"
		sinasc_path <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/" 
		sisprenatal_path <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SISPRENATAL/"
	
		sim_files <- recursive_ftp_scrape( sim_path )
		sinasc_files <- recursive_ftp_scrape( sinasc_path )
		sisprenatal_files <- recursive_ftp_scrape( sisprenatal_path )

		catalog <-
			data.frame(
				full_url = c( sim_files , sinasc_files , sisprenatal_files ) ,
				stringsAsFactors = FALSE
			)

		catalog$type <-
			ifelse( grepl( sim_path , catalog$full_url ) , "sim" ,
			ifelse( grepl( sinasc_path , catalog$full_url ) , "sinasc" ,
			ifelse( grepl( sisprenatal_path , catalog$full_url ) , "sisprenatal" , NA ) ) )
			
		catalog$output_filename <- 
			gsub( "dados/" , "" , 
			gsub( "201201_/" , "" , 
			gsub( "dbc$" , "rda" , 
			gsub( "ftp://ftp.datasus.gov.br/dissemin/publicos/" , paste0( output_dir , "/" ) , 
				tolower( catalog$full_url ) 
				) , 
				ignore.case = TRUE )
				)
				)

		year_lines <- gsub( "[^0-9]" , "" , basename( catalog$full_url ) )

		catalog$year <-
			ifelse( nchar( year_lines ) == 2 & as.numeric( year_lines ) < 79 , 2000 + as.numeric( year_lines ) ,
			ifelse( nchar( year_lines ) == 2 & as.numeric( year_lines ) >= 79 , 1900 + as.numeric( year_lines ) ,
			ifelse( nchar( year_lines ) == 4 & as.numeric( year_lines ) >= 1996 , as.numeric( year_lines ) ,
			ifelse( nchar( year_lines ) == 4 & as.numeric( year_lines ) < 1996 , 2000 + as.numeric( substr( year_lines , 1 , 2 ) ) , NA ) ) ) )
			
			
		catalog$db_tablename <-
			ifelse( !grepl( "dbc$" , catalog$full_url , ignore.case = TRUE ) , NA ,
			ifelse( grepl( "/dofet" , catalog$output_filename ) , 
				paste0( substr( basename( catalog$output_filename ) , 3 , 5 ) , ifelse( grepl( "/cid9" , catalog$output_filename ) , "_cid9" , "_cid10" ) ) ,
			ifelse( grepl( "/dores" , catalog$output_filename ) , 
				paste0( "geral" , ifelse( grepl( "/cid9" , catalog$output_filename ) , "_cid9" , "_cid10" ) ) ,
			ifelse( grepl( "/sinasc" , catalog$output_filename ) , 
				ifelse( grepl( "/dnign" , catalog$output_filename ) , "nign" ,
				paste0( "nasc" , ifelse( grepl( "/ant" , catalog$output_filename ) , "_cid9" , "_cid10" ) ) ) ,
			ifelse( grepl( "/sisprenatal" , catalog$output_filename ) , "pn" , 
			ifelse( grepl( "doign" , catalog$output_filename ) , "dign" , NA ) ) ) ) ) )
			
		catalog$dbfolder <- ifelse( is.na( catalog$db_tablename ) , NA , paste0( output_dir , "/MonetDB" ) )
	  
		catalog

	}



lodown_datasus <-
  function( data_name = "datasus" , catalog , ... ){

    if ( !requireNamespace( "read.dbc" , quietly = TRUE ) ) stop( "read.dbc needed for this function to work. to install it, special `install.packages( 'read.dbc' )`" , call. = FALSE )

    tf <- tempfile()

    for ( i in seq_len( nrow( catalog ) ) ){

		
      # download the file
      cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

	  if( !grepl( "dbc$" , catalog[ i , 'full_url' ] , ignore.case = TRUE ) ){
	  
		file.copy( tf , catalog[ i , 'output_filename' ] )
		
	} else {
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

			}

			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

		}
		
        
      }


      # delete the temporary files
      suppressWarnings( file.remove( tf ) )

      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

    }

    catalog

  }

