get_catalog_censo_escolar <-
	function( data_name = "censo_escolar" , output_dir , ... ){

		inep_portal <- "http://portal.inep.gov.br/basica-levantamentos-acessar"

		portal_table <- rvest::html_table( xml2::read_html( inep_portal ) , fill = TRUE )[[1]]

		year_lines <- portal_table[ portal_table$X1 == "Microdados Censo Escolar" , 'X2' ]

		year_lines <- iconv( year_lines , "" , "ASCII" , sub = " " )

		year_lines <- gsub( " +" , " " , year_lines )

		censo_escolar_years <- strsplit( year_lines , " " )[[1]]

		catalog <-
			data.frame(
				year = censo_escolar_years ,
				full_url = NA ,
				db_tablename = paste0( "x" , censo_escolar_years ) ,
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				output_folder = paste0( output_dir , "/" , censo_escolar_years ) ,
				stringsAsFactors = FALSE
			)

		# get real full_urls
		w <- rvest::html_attr(
		  rvest::html_nodes( xml2::read_html( "http://portal.inep.gov.br/basica-levantamentos-acessar" ) , "a" ) , "href" )

		censoesc_files <- grep( "censo_escolar.*zip$" , w , value = TRUE )
		catalog$full_url <- censoesc_files

		# have not completed testing prior to 2008
		catalog <- catalog[ catalog$year >= 2008 , ]
		
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

			unzipped_files <- unzip( tf , exdir = catalog[ i , "output_folder" ] )

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
						paste0( this_table_type , catalog[ i , "year" ] ) ,
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

			# delete the temporary files?  or move some docs to a save folder?
			suppressWarnings( file.remove( tf ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

