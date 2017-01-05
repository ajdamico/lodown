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

		catalog

	}


lodown_censo_escolar <-
	function( data_name = "censo_escolar" , catalog , path_to_7z = if( .Platform$OS.type != 'windows' ) '7z' else normalizePath( "C:/Program Files/7-zip/7z.exe" ) , ... ){

	  if( system( paste0( '"' , path_to_7z , '" -h' ) , show.output.on.console = FALSE ) != 0 ) stop( paste0( "you need to install 7-zip.  if you already have it, include a parameter like path_to_7z='" , path_to_7z , "'" ) )

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )

			rar_files <- grep( ".rar$", unzipped_files, value = TRUE )

			for( table.data in c( "docente", "matricula", "turma", "escola" ) ) {

			  tabelas <- grep( table.data, rar_files, value = TRUE, ignore.case = TRUE )

			  for ( j in seq_along( tabelas ) ) {

			    # build the string to send to DOS
			    dos.command <- paste0( '"' , path_to_7z , '" x "' , normalizePath( tabelas[ j ] ) , '" -o"' , normalizePath( paste0( tempdir() , '\\unzips' ) ) , '"' )

			    # extract the file
			    system( dos.command , show.output.on.console = FALSE )

			    # get csv data file:
			    data.file <- list.files( path = paste0( tempdir() , '\\unzips' ), full.names = TRUE )
			    data.file <- grep( "csv", data.file, value = TRUE, ignore.case = TRUE )

			    # read csv into monetdb database
			    if ( length( tabelas ) == 1 ) {
			      DBI::dbWriteTable( db,
			                         name = paste0( table.data, catalog[ i , "year" ] ),
			                         data.file,
			                         sep = "|",
			                         best.effort = TRUE,
			                         lower.case.names = TRUE,
			                         nrow.check = 5000 )
			    } else {
			      DBI::dbWriteTable( db,
			                         name = paste0( table.data, catalog[ i , "year" ], "_", j ),
			                         data.file,
			                         sep = "|",
			                         best.effort = TRUE,
			                         lower.case.names = TRUE,
			                         nrow.check = 5000 )
			    }

			    unlink( c( data.file ) )

			  }

			}

			# now the files are unzipped.. read them into the monetdb table using tablename

				# catalog[ i , 'db_tablename' ]


			# also save the documentation in the
				# catalog[ i , 'output_folder' ]


			# delete the temporary files?  or move some docs to a save folder?
			# suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

