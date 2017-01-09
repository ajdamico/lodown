get_catalog_enem <-
	function( data_name = "enem" , output_dir , ... ){

		inep_portal <- "http://portal.inep.gov.br/basica-levantamentos-acessar"

		portal_table <- rvest::html_table( xml2::read_html( inep_portal ) , fill = TRUE )[[1]]

		year_lines <- portal_table[ portal_table$X1 == "Microdados Enem" , 'X2' ]

		year_lines <- iconv( year_lines , "" , "ASCII" , sub = " " )

		year_lines <- gsub( " +" , " " , year_lines )

		enem_years <- strsplit( year_lines , " " )[[1]]

		catalog <-
			data.frame(
				year = enem_years ,
				full_url = NA ,
				db_tablename = paste0( "x" , enem_years ) ,
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				output_folder = paste0( output_dir , "/" , enem_years ) ,
				stringsAsFactors = FALSE
			)

		# get real full_urls
		w <- rvest::html_attr( rvest::html_nodes( xml2::read_html( inep_portal ) , "a" ) , "href" )

		catalog$full_url <- w[ grep( "enem(.*)zip$|enem(.*)rar$" , basename( w ) , ignore.case = TRUE ) ]

		catalog

	}


lodown_enem <-
	function( data_name = "enem" , catalog , path_to_7z = if( .Platform$OS.type != 'windows' ) '7za' else normalizePath( "C:/Program Files/7-zip/7z.exe" ) , ... ){

		if ( !requireNamespace( "readxl" , quietly = TRUE ) ) stop( "readxl needed for this function to work. to install it, type `install.packages( 'readxl' )`" , call. = FALSE )

		if( system( paste0( '"' , path_to_7z , '" -h' ) , show.output.on.console = FALSE ) != 0 ) stop( paste0( "you need to install 7-zip.  if you already have it, include a parameter like path_to_7z='" , path_to_7z , "'" ) )

		tf <- tempfile() ; tf2 <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			if( !grepl( "\\.rar" , catalog[ i , "full_url" ] ) ){

				dos.command <- paste0( '"' , path_to_7z , '" x "' , tf , '" -o"' , normalizePath( catalog[ i , "output_folder" ] ) , '"' )

				system( dos.command )

				z <- unique( list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) )

				zf <- grep( "\\.zip|\\.ZIP" , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) , value = TRUE )

				if( length( zf ) > 0 ){

					dos.command <- paste0( '"' , path_to_7z , '" x "' , zf , '" -o"' , normalizePath( catalog[ i , "output_folder" ] ) , '"' )

					system( dos.command )

					z <- unique( c( z , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) ) )

				}

				rfi <- grep( "\\.rar|\\.RAR" , z , value = TRUE )

			} else {
			
				z <- NULL
			
				rfi <- tf
			
			}

			if( length( rfi ) > 0 ) {

				dos.command <- paste0( '"' , path_to_7z , '" x "' , rfi , '" -o"' , normalizePath( catalog[ i , "output_folder" ] ) , '"' )
				
				system( dos.command )

				z <- unique( c( z , list.files( catalog[ i , 'output_folder' ] , recursive = TRUE , full.names = TRUE  ) ) )

			}

			# 2007 has a duplicate
			if( any( grepl( "DADOS(.*)DADOS_ENEM_2007\\.TXT" , z ) ) ) z <- z[ !grepl( "DADOS(.*)DADOS_ENEM_2007\\.TXT" , z ) ]

			csvfile <- grep( "\\.csv|\\.CSV" , z , value = TRUE )

			# save school math perfomance 

			xlsfile <- grep( "\\.xls|\\.XLS" , z , value = TRUE )

			school_file <- grep( "PLAN" , xlsfile , value = TRUE )

			if( length( school_file ) > 0 ){
				
				x <- readxl::read_excel( school_file , sheet = 3 , skip = 1 )
				
				names( x ) <- tolower( names( x ) )
				
				save( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , paste0( "mat" , catalog[ i , 'year' ] , ".rda" ) ) )
				
			}


			if( length( csvfile ) > 0 ){

				for( this_file in csvfile ){

					tablename <- tolower( gsub( "\\.(.*)" , "" , basename( this_file ) ) )

					soc <- grepl( "," , readLines( this_file , 1 ) )

					attempt_one <- try( monetdb.read.csv( db , this_file , tablename , lower.case.names = TRUE , delim = ifelse( soc , "," , ";" ) ) , silent = TRUE )

					if( class( attempt_one ) == 'try-error' ){

						this_file <- enem_ranc( this_file )

						monetdb.read.csv( db , this_file , tablename , lower.case.names = TRUE , delim = ifelse( soc , "," , ";" ) , best.effort = tablename == "microdados_enem_2013" )

					}

					stopifnot( R.utils::countLines( this_file ) %in% ( DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , tablename ) )[ 1 , 1 ] + -5:5 ) )

				}

			} else {

				sas_ri <- grep( "\\.sas|\\.SAS" , z , value = TRUE )

				if( length( sas_ri ) > 1 ) sas_ri <- sas_ri[ !grepl( "questionario|prova" , tolower( basename( sas_ri ) ) ) ]

				# if( catalog[ i , 'year' ] %in% 1999:2000 ) options( encoding = 'native.enc' )
				sas_t <- readLines( sas_ri )
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
						tablename = catalog[ i , 'db_tablename' ] ,
						conn = db
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
							tablename = catalog[ i , 'db_tablename' ] ,
							conn = db ,
							try_best_effort = TRUE
						)

					} else {

						read_SAScii_monetdb( 
							dfile , 
							tf2 , 
							zipped = FALSE , 
							tl = TRUE ,
							tablename = catalog[ i , 'db_tablename' ] ,
							conn = db
						)

					}

				}

				if( row_check ) stopifnot( R.utils::countLines( dfile ) %in% ( DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , catalog[ i , 'db_tablename' ] ) )[ 1 , 1 ] + -5:5 ) )

			}


			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			# delete the temporary files?  or move some docs to a save folder?
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

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

