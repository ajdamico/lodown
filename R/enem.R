get_catalog_enem <-
	function( data_name = "enem" , output_dir , ... ){

		if ( !requireNamespace( "archive" , quietly = TRUE ) ) stop( "archive needed for this function to work. to install it, type `devtools::install_github( 'jimhester/archive' )`" , call. = FALSE )

		inep_portal <- "http://portal.inep.gov.br/microdados"

		w <- rvest::html_attr( rvest::html_nodes( xml2::read_html( inep_portal ) , "a" ) , "href" )
		
		these_links <- w[ grep( "enem(.*)zip$|enem(.*)rar$" , basename( w ) , ignore.case = TRUE ) ]

		enem_years <- substr( gsub( "[^0-9]" , "" , these_links ) , 1 , 4 )

		catalog <-
			data.frame(
				year = enem_years ,
				full_url = these_links ,
				dbfile = paste0( output_dir , "/SQLite.db" ) ,
				output_folder = paste0( output_dir , "/" , enem_years ) ,
				stringsAsFactors = FALSE
			)

		# skip 2009 and 2010 until
		# https://github.com/ajdamico/asdfree/issues/265
		catalog <- subset( catalog , !( year %in% 2009:2010 ) )
		
		# figure out and fix 2012 and 2013
		# https://github.com/ajdamico/asdfree/issues/296
		catalog <- subset( catalog , !( year %in% 2012:2013 ) )
		
		catalog

	}


lodown_enem <-
	function( data_name = "enem" , catalog , ... ){

		on.exit( print( catalog ) )

		if ( !requireNamespace( "readxl" , quietly = TRUE ) ) stop( "readxl needed for this function to work. to install it, type `install.packages( 'readxl' )`" , call. = FALSE )

		tf <- tempfile() ; tf2 <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			if( !grepl( "\\.rar" , catalog[ i , "full_url" ] ) ){

				if( catalog[ i , 'year' ] < 2005 ){
				
					unzip( tf , exdir = normalizePath( catalog[ i , "output_folder" ] ) )

				} else {

					archive::archive_extract( tf , dir = normalizePath( catalog[ i , "output_folder" ] ) )

				}
				
				
				z <- unique( list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) )

				zf <- grep( "\\.zip|\\.ZIP" , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) , value = TRUE )

				if( length( zf ) > 0 ){

					if( catalog[ i , 'year' ] < 2009 ){
					
						unzip( zf , exdir = normalizePath( catalog[ i , "output_folder" ] ) )

					} else {

						archive::archive_extract( zf , dir = normalizePath( catalog[ i , "output_folder" ] ) )

					}

					z <- unique( c( z , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) ) )

				}

				rfi <- grep( "\\.rar|\\.RAR" , z , value = TRUE )

			} else {
			
				z <- NULL
			
				rfi <- tf
			
			}

			if( length( rfi ) > 0 ) {

				if( catalog[ i , 'year' ] < 2009 ){
				
					unzip( rfi , exdir = normalizePath( catalog[ i , "output_folder" ] ) )

				} else {

					archive::archive_extract( rfi , dir = normalizePath( catalog[ i , "output_folder" ] ) )

				}

				z <- unique( c( z , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) ) )

			}

			# 2007 has a duplicate
			if( any( grepl( "DADOS(.*)DADOS_ENEM_2007\\.TXT" , z ) ) ) z <- z[ !grepl( "DADOS(.*)DADOS_ENEM_2007\\.TXT" , z ) ]

			csvfile <- grep( "\\.csv|\\.CSV" , z , value = TRUE )

			if( length( csvfile ) > 0 ){

				for( this_file in csvfile ){

					tablename <- gsub( "^dados_" , "" , tolower( gsub( "\\.(.*)" , "" , basename( this_file ) ) ) )

					soc <- grepl( "," , readLines( this_file , 1 ) )

					attempt_one <- try( MonetDBLite::monetdb.read.csv( db , this_file , tablename , lower.case.names = TRUE , delim = ifelse( soc , "," , ";" ) ) , silent = TRUE )

					if( class( attempt_one ) == 'try-error' ){

						this_file <- enem_ranc( this_file )

						MonetDBLite::monetdb.read.csv( db , this_file , tablename , lower.case.names = TRUE , delim = ifelse( soc , "," , ";" ) , best.effort = tablename == "microdados_enem_2013" )

					}

					stopifnot( R.utils::countLines( this_file ) %in% ( DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , tablename ) )[ 1 , 1 ] + -5:5 ) )

				}

			} else {

				sas_ri <- grep( "\\.sas|\\.SAS" , z , value = TRUE )

				if( length( sas_ri ) > 1 ) sas_ri <- sas_ri[ !grepl( "questionario|prova" , tolower( basename( sas_ri ) ) ) ]

				# if( catalog[ i , 'year' ] %in% 1999:2000 ) options( encoding = 'native.enc' )
				sas_con <- file( sas_ri , "r" , encoding = 'windows-1252' )
				sas_t <- readLines( sas_con )
				sas_t <- gsub( "\t" , " " , sas_t )
				sas_t <- gsub( "char(.*)" , "\\1" , tolower( sas_t ) )
				sas_t <- gsub( "datetime(.*)" , "$ \\1" , tolower( sas_t ) )
				sas_t <- gsub( "\U0096" , " " , sas_t )
				sas_t <- iconv( sas_t , "" , "ASCII" , sub = " " )
				writeLines( sas_t , tf2 )
				# if( catalog[ i , 'year' ] %in% 1999:2000 ) options( encoding = 'latin1' )

				dfile <- grep( "\\.txt|\\.TXT" , z , value = TRUE )

				if( length( dfile ) > 1 ) dfile <- dfile[ grep( "dados" , tolower( basename( dfile ) ) ) ]

				row_check <- TRUE

				attempt_one <- try( {
				
					read_SAScii_monetdb( 
						dfile , 
						tf2 , 
						zipped = FALSE , 
						tl = TRUE ,
						tablename = paste0( "enem" , catalog[ i , 'year' ] ) ,
						connection = db
					)
					
				} , silent = TRUE )

				if( class( attempt_one ) == 'try-error' ){

					dfile <- enem_ranc( dfile )

					if( catalog[ i , 'year' ] == 2004 ){

						row_check <- FALSE

						read_SAScii_monetdb( 
							dfile , 
							tf2 , 
							zipped = FALSE , 
							tl = TRUE ,
							tablename = paste0( "enem" , catalog[ i , 'year' ] ) ,
							connection = db ,
							try_best_effort = TRUE
						)

					} else {

						read_SAScii_monetdb( 
							dfile , 
							tf2 , 
							zipped = FALSE , 
							tl = TRUE ,
							tablename = paste0( "enem" , catalog[ i , 'year' ] ) ,
							connection = db
						)

					}

				}

				if( row_check ) stopifnot( R.utils::countLines( dfile ) %in% ( DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , paste0( "enem" , catalog[ i , 'year' ] ) ) )[ 1 , 1 ] + -5:5 ) )

			}


			catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM enem" , catalog[ i , 'year' ] ) )[ 1 , 1 ]
			
			# delete the temporary files?  or move some docs to a save folder?
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfile' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}


	
	

enem_ranc <- 
	function( infile ){

		tf_a <- tempfile()

		outcon <- file( tf_a , "w" )

		incon <- file( infile , "r")

		line.num <- 0
			
		while( length( line <- readLines( incon , 1 ) ) > 0 ){
			
			line <- iconv( line , "" , "ASCII" , sub = " " )
			
			line <- gsub( " ." , "  " , line , fixed = TRUE )
			line <- gsub( ". " , "  " , line , fixed = TRUE )
			line <- gsub( " ." , "  " , line , fixed = TRUE )
			line <- gsub( ". " , "  " , line , fixed = TRUE )
			line <- gsub( " ." , "  " , line , fixed = TRUE )
			line <- gsub( ". " , "  " , line , fixed = TRUE )
			line <- gsub( " ." , "  " , line , fixed = TRUE )
			line <- gsub( ". " , "  " , line , fixed = TRUE )
			line <- gsub( " ." , "  " , line , fixed = TRUE )
			line <- gsub( ". " , "  " , line , fixed = TRUE )
			
			writeLines( line , outcon )
		}

		close( incon )

		close( outcon )

		tf_a
	}

