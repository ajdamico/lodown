get_catalog_censo_escolar <-
	function( data_name = "censo_escolar" , output_dir , ... ){

		inep_portal <- "http://portal.inep.gov.br/microdados"

		w <- rvest::html_attr( rvest::html_nodes( xml2::read_html( inep_portal ) , "a" ) , "href" )

		these_links <- grep( "censo_escolar(.*)zip$" , w , value = TRUE , ignore.case = TRUE )

		censo_escolar_years <- substr( gsub( "[^0-9]" , "" , these_links ) , 1 , 4 )
		
		catalog <-
			data.frame(
				year = censo_escolar_years ,
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				output_folder = paste0( output_dir , "/" , censo_escolar_years ) ,
				full_url = these_links ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_censo_escolar <-
	function( data_name = "censo_escolar" , catalog , path_to_7z = if( .Platform$OS.type != 'windows' ) '7za' else normalizePath( "C:/Program Files/7-zip/7z.exe" ) , ... ){

	  if( system( paste0( '"' , path_to_7z , '" -h' ) , show.output.on.console = FALSE ) != 0 ) stop( paste0( "you need to install 7-zip.  if you already have it, include a parameter like path_to_7z='" , path_to_7z , "'" ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = catalog[ i , "output_folder" ] )

			for( these_zips in grep( "\\.zip$" , unzipped_files , value = TRUE , ignore.case = TRUE ) ) unzipped_files <- c( unzipped_files , unzip_warn_fail( these_zips , exdir = dirname( these_zips ) ) )
			
			if( catalog[ i , 'year' ] <= 2006 ){
			
				sas_files <- grep( "\\.sas$", unzipped_files, value = TRUE , ignore.case = TRUE )
			
				sas_scaledowns <- gsub( "SAS|_" , "" , gsub( "\\.sas|\\.SAS" , "" , gsub( paste0( "INPUT|" , catalog[ i , 'year' ] ) , "" , basename( sas_files ) ) ) )
				sas_scaledowns <- ifelse( grepl( "ESC" , sas_scaledowns ) & !grepl( "INDICESC" , sas_scaledowns ) , gsub( "ESC" , "" , sas_scaledowns ) , sas_scaledowns )
				
				datafile_matches <- lapply( sas_scaledowns , function( z ) unzipped_files[ grepl( z , basename( unzipped_files ) , ignore.case = TRUE ) & grepl( "dados" , dirname( unzipped_files ) , ignore.case = TRUE ) ] )
				datafile_matches <- lapply( datafile_matches , function( z ) z[ !grepl( "\\.zip" , z , ignore.case = TRUE ) ] )
				
				these_tables <- 
					data.frame( 
						sas_script = sas_files , 
						data_file = unique( unlist( datafile_matches ) ) , 
						db_tablename = paste0( tolower( sas_scaledowns ) , "_" , catalog[ i , 'year' ] ) , 
						stringsAsFactors = FALSE 
					)
				
				for( j in seq( nrow( these_tables ) ) ){

					Encoding( these_tables[ j , 'sas_script' ] ) <- ''
				
					# write the file to the disk
					w <- readLines( these_tables[ j , 'sas_script' ] )
					
					# remove all tab characters
					w <- gsub( '\t' , ' ' , w )
					
					w <- gsub( '@ ' , '@' , w , fixed = TRUE )
					w <- gsub( "@371 CEST_SAUDE  " , "@371 CEST_SAUDE $1 " , w , fixed = TRUE )
					w <- gsub( "@379 OUTROS  " , "@379 OUTROS $1 " , w , fixed = TRUE )
					w <- gsub( "VEF918 11" , "VEF918 11" , w , fixed = TRUE )
					w <- gsub( "VEE1411( +)/" , "VEE1411 7. /" , w )
					w <- gsub( "VEE1412( +)/" , "VEE1412 7. /" , w )
					
					# overwrite the file on the disk with the newly de-tabbed text
					writeLines( w , these_tables[ j , 'sas_script' ] )

					if( R.utils::countLines( these_tables[ j , 'data_file' ] ) < 1000000 ){
					
						x <- data.frame( read_SAScii( these_tables[ j , 'data_file' ] , these_tables[ j , 'sas_script' ] , na = c( "" , "." ) ) )
						
						# convert column names to lowercase
						names( x ) <- tolower( names( x ) )
						
						# do not use monetdb reserved words
						for ( k in names( x )[ toupper( names( x ) ) %in% getFromNamespace( "reserved_monetdb_keywords" , "MonetDBLite" ) ] ) names( x )[ names( x ) == k ] <- paste0( k , "_" )

						DBI::dbWriteTable( db , these_tables[ j , 'db_tablename' ] , x )
						
						rm( x )
					
					} else {
						
						read_SAScii_monetdb(
							these_tables[ j , 'data_file' ] ,
							these_tables[ j , 'sas_script' ] ,
							tl = TRUE ,
							tablename = these_tables[ j , 'db_tablename' ] ,
							connection = db ,
							na_strings = ''
						)
					
					}
						
					catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , DBI::dbGetQuery( db , paste( "SELECT COUNT(*) FROM" , these_tables[ j , 'db_tablename' ] ) )[ 1 , 1 ] )
				
				}
				
			} else {
			
				rar_files <- grep( "\\.rar$", unzipped_files, value = TRUE , ignore.case = TRUE )

				for( this_table_type in c( "docente", "matricula", "turma", "escola" ) ) {

					tabelas <- grep( this_table_type , rar_files, value = TRUE, ignore.case = TRUE )

					for ( j in seq_along( tabelas ) ) {

						# build the string to send to DOS
						dos.command <- paste0( '"' , path_to_7z , '" x "' , normalizePath( tabelas[ j ] ) , '" -o"' , normalizePath( catalog[ i , "output_folder" ] ) , '"' )

						system( dos.command , show.output.on.console = FALSE )

						this_data_file <- list.files( catalog[ i , "output_folder" ] , full.names = TRUE )

						this_data_file <- grep( "\\.csv$", this_data_file, value = TRUE, ignore.case = TRUE )

						DBI::dbWriteTable(
							db,
							paste0( this_table_type , "_" , catalog[ i , "year" ] ) ,
							this_data_file ,
							sep = "|" ,
							best.effort = TRUE ,
							lower.case.names = TRUE ,
							append = TRUE ,
							nrow.check = 1000
						)

						file.remove( this_data_file )

					}

				}

				catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM matricula_" , catalog[ i , "year" ] ) )[ 1 , 1 ]

			}
			
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			# delete the temporary files?  or move some docs to a save folder?
			suppressWarnings( file.remove( tf ) )

			# remove raw data files already loaded
			for ( rm.dir in grep( "/DADOS/.*", unzipped_files , value = TRUE , ignore.case = TRUE ) ) {
			  suppressWarnings( unlink( rm.dir , recursive = TRUE )  )
			}

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}



		catalog

	}

