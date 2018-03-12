get_catalog_pof <-
	function( data_name = "pof" , output_dir , ... ){

		if ( !requireNamespace( "archive" , quietly = TRUE ) ) stop( "archive needed for this function to work. to install it, type `devtools::install_github( 'jimhester/archive' )`" , call. = FALSE )

		pof_ftp <- "ftp://ftp.ibge.gov.br/Orcamentos_Familiares/"

		ftp_listing <- readLines( textConnection( RCurl::getURL( pof_ftp ) ) )

		ay <- rev( gsub( "(.*) (.*)" , "\\2" , ftp_listing ) )

		# hardcoded removal of microdata before 2003
		ay <- ay[ !( ay %in% c( "" , "Pesquisa_de_Orcamentos_Familiares_1987_1988" , "Pesquisa_de_Orcamentos_Familiares_1995_1996" , "Pesquisa_de_Orcamentos_Familiares_1997_1998" ) ) ]

		second_year <- gsub( "(.*)_([0-9]+)" , "\\2" , ay )

		catalog <-
			data.frame(
				full_urls = paste0( pof_ftp , ay , "/Microdados/Dados.zip" ) ,
				period = gsub( "Pesquisa_de_Orcamentos_Familiares_" , "" , ay ) ,
				documentation = paste0( pof_ftp , ay , "/Microdados/" , ifelse( second_year < 2009 , "Documentacao.zip" , "documentacao.zip" ) ) ,
				aliment_file = ifelse( second_year < 2009 , NA , paste0( pof_ftp , ay , "/Microdados/tradutores.zip" ) ) ,
				output_folder = paste0( output_dir , "/" , gsub( "Pesquisa_de_Orcamentos_Familiares_" , "" , ay ) ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_pof <-
	function( data_name = "nppes" , catalog , ... ){

		on.exit( print( catalog ) )

		if ( !requireNamespace( "readxl" , quietly = TRUE ) ) stop( "readxl needed for this function to work. to install it, type `install.packages( 'readxl' )`" , call. = FALSE )

		tf <- tempfile() ; tf2 <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			cachaca( catalog[ i , "full_urls" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			cachaca( catalog[ i , "documentation" ] , tf , mode = 'wb' )

			doc_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			files <- c( unzipped_files , doc_files )

			Encoding( files ) <- 'latin1'

			if( !is.na( catalog[ i , 'aliment_file' ] ) ){

				cachaca( catalog[ i , "aliment_file" ] , tf , mode = 'wb' )

				ali_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

				Encoding( ali_files ) <- 'latin1'

				# # # # # # # # # # # # # #
				# tables with food codes  #

				# figure out which is the alimentacao file
				cda <- ali_files[ grep( 'codigos_de_alimentacao' , tolower( ali_files ) ) ]

				# extract both tabs from the excel file
				componentes <- readxl::read_excel( cda , sheet = 1 , skip = 3 , col_types = rep( 'text' , 7 ) , col_names = c( 'codigo' , 'nivel.1' , 'desc.1' , 'nivel.2' , 'desc.2' , 'nivel.3' , 'desc.3' ) )
				componentes <- as.data.frame( componentes , stringsASFactors = FALSE )
				componentes[ , 1:2 ] <- apply( componentes[ , 1:2 ] , 2 , function(x) { gsub( "\\..*", "" , x ) } )
				estrutura <- readxl::read_excel( cda , sheet = 2 , skip = 3 , col_types = rep( 'text' , 6 ) , col_names = c( 'nivel.1' , 'desc.1' , 'nivel.2' , 'desc.2' , 'nivel.3' , 'desc.3' ) )
				estrutura <- as.data.frame( estrutura , stringsASFactors = FALSE )
				estrutura[ , 1 ] <- gsub( "\\..*", "" , estrutura[ , 1 ] )

				# componentes table has a footnote, so throw it out
				# by removing all records with a missing
				# or empty `nivel.1` field
				componentes <- componentes[ !is.na( componentes$nivel.1 ) , ]
				componentes <- componentes[ componentes$nivel.1 != "" , ]


				# save both of these data frames to the local disk
				saveRDS( componentes , file = paste0( catalog[ i , 'output_folder' ] , "/codigos de alimentacao componentes.rds" ) , compress = FALSE )

				saveRDS( estrutura , file = paste0( catalog[ i , 'output_folder' ] , "/codigos de alimentacao estrutura.rds" ) , compress = FALSE )

				# # # # # # # # # # # # # # # # #
				# table for post-stratification #

				# figure out which is the post-stratification table
				pos <- files[ grep( 'pos_estratos_totais' , tolower( files ) ) ]

				# extract the post-stratification table
				# from the excel file
				poststr <- readxl::read_excel( pos , sheet = 1 , col_names = c( "uf",	"control", "estrato", "fator_des" ,	"tot_pop", "estrato_unico",	"pos_estrato", "tot_unidade_c", "fator_pos" ) , col_types = rep( "numeric" , 9 ) , skip = 1 )
				poststr <- as.data.frame( poststr , stringsASFactors = FALSE )
				# imported!  cool?  cool.

				# convert all column names to lowercase
				names( poststr ) <- tolower( names( poststr ) )

				# save this data frame to the local disk
				saveRDS( poststr , file = paste0( catalog[ i , 'output_folder' ] , "/poststr.rds" ) , compress = FALSE )

				# remove all three of these tables from memory
				rm( componentes , estrutura , poststr )

			}





			# # # # # # # # # # # # # #
			# sas import organization #

			# before you worry about the data files,
			# get the sas import scripts under control.

			# extract the leitura file containing the sas importation instructions
			leitura <- files[ grep( 'leitura' , tolower( files ) ) ]

			# read the whole thing into memory
			leitura_con <- file( leitura , encoding = 'windows-1252' )

			z <- readLines( leitura_con )

			# remove all those goofy tab characters (which will screw up SAScii)
			z <- gsub( "\t" , " " , z )

			# remove lines containing the `if reg=__ then do;` pattern
			z <- z[ !grepl( 'if reg=.* then do;' , z ) ]

			# remove goofy @;
			z <- gsub( "@;" , "" , z )

			# remove goofy "/;";
			z <- gsub( "/;" , "/" , z )

			# remove lines containing solely `input`
			z <- z[ !( tolower( z ) == 'input' ) ]

			# remove the (SAScii-breaking) overlapping `controle` columns
			z <- z[ !grepl( "@3 controle 6." , z , fixed = TRUE ) ]

			# write the file back to your second temporary file
			writeLines( z , tf2 )

			# find each of your beginline parameters

			# find each line containing the string `INFILE` or `infile`
			all.beginlines <- grep( 'INFILE|infile' , z )

			# find line start positions
			start.pos <-
				unlist(
					lapply(
						gregexpr(
							"\\" ,
							z[ all.beginlines ] ,
							fixed = TRUE
						) ,
						max
					)
				) + 1

			# find line end positions
			end.pos <-
				unlist(
						gregexpr(
							".txt" ,
							z[ all.beginlines ]
						)
					) - 1

			# isolate the names of all data files to be imported..
			data.files.to.import <-
				# pull the 14th character until `.txt` in the `INFILE` lines of the sas import script
				substr(
					z[ all.beginlines ] ,
					start.pos ,
					end.pos
				)

			# now you've got an object containing the names of all data files that need to be imported.
			data.files.to.import

			# isolate the base filename before the period
			# for all downloaded files..
			all.file.basenames <-
				unlist(
					lapply(
						strsplit(
							basename( files ) ,
							'.' ,
							fixed = TRUE
						) ,
						'[[' ,
						1
					)
				)

			# for each data file name in `data.files.to.import`..
			for ( dfn in data.files.to.import ){

				# identify which .7z file contains the data
				if ( tolower( dfn ) == 't_rendimentos' ) {
					data.file <- files[ which( 't_rendimentos1' == tolower( all.file.basenames ) ) ]
				} else {
					data.file <- files[ which( tolower( dfn ) == tolower( all.file.basenames ) ) ]
				}


				# if `data.file` contains multiple files..
				if ( length( data.file ) > 1 ){

					# pick the zipped file..
					data.file <- data.file[ grep( '.zip' , tolower( data.file ) , fixed = TRUE ) ]

					# ..unzip it, and overwrite `data.file` with the new filepath
					data.file <- unzip_warn_fail( data.file , exdir = tempdir() )
				}


				# and now, if the data.file is just a text file..
				if ( grepl( "txt$" , tolower( data.file ) ) ){

					# then no unzipping is necessary
					curfile <- data.file

				# otherwise, the file must be unzipped with 7-zip
				} else {

					archive::archive_extract( normalizePath( data.file ) , dir = file.path( tempdir() , 'unzips' ) )

					# find the name of the final ASCII data file to be imported
					curfile <- paste0( tempdir() , '/unzips/' , gsub( ".7z" , ".txt" , basename( data.file ) ) )

				}

				# figure out which beginline position to use
				cur.beginline <- which( tolower( dfn ) == tolower( data.files.to.import ) )

				curfile_con <- file( curfile , encoding = 'windows-1252' )

				# import the data file into R
				x <-
					read_SAScii(
						curfile_con ,
						tf2 ,
						beginline = all.beginlines[ cur.beginline ] ,
						skip_decimal_division = TRUE ,
						sas_encoding = "latin1"
					)

				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )

				catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , nrow( x ) , na.rm = TRUE )

				# save the current data.frame
				# to the appropriate year folder
				# within the current working directory
				saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , "/" , tolower( dfn ) , ".rds" ) , compress = FALSE )

				# delete the current file from the current working directory
				file.remove( curfile )

			}

			# revert the encoding for more effective deletion.
			Encoding( files ) <- Encoding( ali_files ) <- ''



			# delete the temporary files
			suppressWarnings( file.remove( tf , files , ali_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

