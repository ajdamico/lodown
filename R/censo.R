get_catalog_censo <-
	function( data_name = "censo" , output_dir , ... ){

		catalog <- NULL

		# designate the location of the 2010 general sample microdata files
		ftp_path_2010 <-	"ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_Gerais_da_Amostra/Microdados/"

		# fetch all available files in the ftp site's directory
		all_files <- RCurl::getURL( ftp_path_2010 , dirlistonly = TRUE )

		# those files are separated by newline characters in the code,
		# so simply split them up into a character vector
		# full of individual zipped file strings
		all_files <- scan( text = all_files , what = "character", quiet = T )

		# remove the two files you don't need to import
		files_to_download_2010 <- all_files[ !( all_files %in% c( '1_Atualizacoes_20160311.txt' , 'Atualizacoes.txt' , 'Documentacao.zip' ) ) ]

		catalog <-
			rbind( 
				catalog ,
				data.frame(
					full_url = paste0( ftp_path_2010 , files_to_download_2010 ) ,
					year = 2010 ,
					db_table_prefix = gsub( ".zip" , "" , files_to_download_2010 , ignore.case = TRUE ) ,
					dbfolder = paste0( output_dir , "/MonetDB" ) ,
					pes_design = paste0( output_dir , "/pes 2010 design.rda" ) ,
					pes_sas = system.file("extdata", "SASinputPes.txt", package = "lodown") ,
					dom_design = paste0( output_dir , "/dom 2010 design.rda" ) ,
					dom_sas = system.file("extdata", "SASinputDom.txt", package = "lodown") ,
					fam_design = NA ,
					fam_sas = NA ,
					dom_ranc = NA ,
					pes_ranc = NA ,
					fam_ranc = NA ,
					fpc1 = 'v0011' ,
					fpc2 = 'v0010' ,
					fpc3 = 'v0001' ,
					fpc4 = 'v0300' ,
					stringsAsFactors = FALSE
				)
			)

			
		# designate the location of the 2000 general sample microdata files
		ftp_path_2000 <-	"ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2000/Microdados/"

		# fetch all available files in the ftp site's directory
		all_files <- RCurl::getURL( ftp_path_2000 , dirlistonly = TRUE )

		# those files are separated by newline characters in the code,
		# so simply split them up into a character vector
		# full of individual zipped file strings
		all_files <- scan( text = all_files , what = "character", quiet = T )

		# remove the two files you don't need to import
		files_to_download_2000 <- all_files[ !( all_files %in% c( '1_Documentacao.zip' , '2_Atualizacoes.txt' , '1_Documentacao_velho.zip' , "2_Atualizacoes_20160309.txt" , "1_Documentacao_20160309.zip" ) ) ]

		catalog <-
			rbind( 
				catalog ,
				data.frame(
					full_url = paste0( ftp_path_2000 , files_to_download_2000 ) ,
					year = 2000 ,
					db_table_prefix = gsub( ".zip" , "" , files_to_download_2000 , ignore.case = TRUE ) ,
					dbfolder = paste0( output_dir , "/MonetDB" ) ,
					pes_design = paste0( output_dir , "/pes 2000 design.rda" ) ,
					pes_sas = system.file("extdata", "LE_PESSOAS.sas", package = "lodown") ,
					dom_design = paste0( output_dir , "/dom 2000 design.rda" ) ,
					dom_sas = system.file("extdata", "LE_DOMIC.sas", package = "lodown") ,
					fam_design = paste0( output_dir , "/fam 2000 design.rda" ) ,
					fam_sas = system.file("extdata", "LE_FAMILIAS.sas", package = "lodown") ,
					dom_ranc = 170 ,
					pes_ranc = 390 ,
					fam_ranc = 118 ,
					fpc1 = "areap" ,
					fpc2 = "p001" ,
					fpc3 = 'v0102' ,
					fpc4 = 'v0300' ,
					stringsAsFactors = FALSE
				)
			)
		
		catalog
	
	}

lodown_censo <-
	function( data_name = "censo" , catalog , ... ){

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )
			
			unzipped_files <- unzip( tf , exdir = tempdir() )

			dom_file <- unzipped_files[ grep( 'DOM|Domicilios' , unzipped_files , useBytes = TRUE ) ]
			pes_file <- unzipped_files[ grep( 'PES|Pessoas' , unzipped_files , useBytes = TRUE ) ]
			fam_file <- unzipped_files[ grep( 'FAM' , toupper( unzipped_files ) , useBytes = TRUE ) ]

			this_dom <- ifelse( !is.na( catalog[ i , 'dom_ranc' ] ) , ranc_censo( dom_file , catalog[ i , 'dom_ranc' ] ) , dom_file )
			this_pes <- ifelse( !is.na( catalog[ i , 'pes_ranc' ] ) , ranc_censo( pes_file , catalog[ i , 'pes_ranc' ] ) , pes_file )
			this_fam <- ifelse( !is.na( catalog[ i , 'fam_ranc' ] ) , ranc_censo( fam_file , catalog[ i , 'fam_ranc' ] ) , fam_file )
			
			
			for( this_dom in dom_file ){
			
				read_SAScii_monetdb (
					this_dom ,
					sas_ri = catalog[ i , 'dom_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_dom' ) ,
					connection = db
				)

			}
			
			
			for( this_pes in pes_file ){
			
				read_SAScii_monetdb (
					this_pes ,
					sas_ri = catalog[ i , 'pes_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_pes' ) ,
					connection = db
				)

			}
			
			for( this_fam in fam_file ){
			
				read_SAScii_monetdb (
					this_fam ,
					sas_ri = catalog[ i , 'fam_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_fam' ) ,
					connection = db
				)

			}
			
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}
		
		# create unique survey designs
		dom_designs <- unique( catalog[ , c( "year" , "dbfolder" , "dom_design" , paste0( 'fpc' , 1:4 ) ) ] )
		pes_designs <- unique( catalog[ , c( "year" , "dbfolder" , "pes_design" , paste0( 'fpc' , 1:4 ) ) ] )
		fam_designs <- unique( catalog[ , c( "year" , "dbfolder" , "fam_design" , paste0( 'fpc' , 1:4 ) ) ] )
		
		names( dom_designs ) <- c( "year" , 'dbfolder' , 'design' , paste0( 'fpc' , 1:4 ) )
		names( pes_designs ) <- c( "year" , 'dbfolder' , 'design' , paste0( 'fpc' , 1:4 ) )
		names( fam_designs ) <- c( "year" , 'dbfolder' , 'design' , paste0( 'fpc' , 1:4 ) )
		
		dom_designs$type <- 'dom'
		pes_designs$type <- 'pes'
		fam_designs$type <- 'fam'
		
		unique_designs <- rbind( dom_designs , pes_designs , fam_designs )
		
		unique_designs <- unique_designs[ !is.na( unique_designs$design ) , ]
		
		for( i in seq_along( nrow( unique_designs ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , unique_designs[ i , 'dbfolder' ] )

			these_tables <- 
				paste0( 
					catalog[ catalog$dbfolder %in% unique_designs[ i , 'dbfolder' ] & catalog[ , paste0( unique_designs[ i , 'type' ] , "_design" ) ] %in% unique_designs[ i , 'design' ] , 'db_table_prefix' ] , 
					"_" , 
					unique_designs[ i , 'type' ] 
				)
			
			this_stack <-
				paste0(
					'create table c' , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					"_" , 
					unique_designs[ i , 'type' ] , 
					'_pre_fpc as (SELECT * FROM ' ,
					paste0( these_tables , collapse = ') UNION ALL (SELECT * FROM ' ) ,
					') WITH DATA'
				)

			DBI::dbSendQuery( db , this_stack )

			this_create <-
				paste0( 'create table c' , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_' , 
					unique_designs[ i , 'type' ] , 
					'_fpc as (select ' , 
					unique_designs[ i , 'fpc1' ] ,
					' , sum( ' ,
					unique_designs[ i , 'fpc2' ] ,
					' ) as sum_fpc2 from c' , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_' , 
					unique_designs[ i , 'type' ] , 
					'_pre_fpc group by ' , 
					unique_designs[ i , 'fpc1' ] ,
					') WITH DATA' )

			DBI::dbSendQuery( db , this_create )


			count_create <-
				paste0(
					'create table c' ,
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_dom_count_' ,
					unique_designs[ i , 'type' ] , 
					' as (select ' ,
					unique_designs[ i , 'fpc3' ] , 
					' , ' ,
					unique_designs[ i , 'fpc4' ] ,
					' , count(*) as dom_count_' ,
					unique_designs[ i , 'type' ] ,
					' from c' ,
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) ,
					'_' ,
					unique_designs[ i , 'type' ] ,
					'_pre_fpc group by ' ,
					unique_designs[ i , 'fpc3' ] , 
					' , ' ,
					unique_designs[ i , 'fpc4' ] ,
					' ) WITH DATA' )

			DBI::dbSendQuery( db , count_create )

			fpc_merge <-
				paste0( 'create table c' ,
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_' ,
					unique_designs[ i , 'type' ] , 
					' as ( select a1.* , b1.dom_count_' ,
					unique_designs[ i , 'type' ] , 
					' from (select a2.* , b2.sum_fpc2 as sum_fpc from c' ,
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_dom_pre_fpc' ,
					' as a2 inner join c' ,
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_fpc as b2 on a2.' ,
					unique_designs[ i , 'fpc1' ] , 
					' = b2.' ,
					unique_designs[ i , 'fpc1' ] , 
					') as a1 inner join c' ,
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_dom_count_' ,
					unique_designs[ i , 'type' ] , 
					' as b1 on a1.' ,
					unique_designs[ i , 'fpc3' ] , 
					' = b1.' ,
					unique_designs[ i , 'fpc3' ] , 
					' AND a1.' ,
					unique_designs[ i , 'fpc4' ] , 
					' = b1.' ,
					unique_designs[ i , 'fpc4' ] , 
					' ) WITH DATA' )
		
			DBI::dbSendQuery( db , fpc_merge )
			
			DBI::dbSendQuery( db , paste0( 'ALTER TABLE c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_' , unique_designs[ i , 'type' ] , ' ADD COLUMN ' , unique_designs[ i , 'type' ] , '_wgt DOUBLE PRECISION' ) )

			DBI::dbSendQuery( db , paste0( 'UPDATE c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_' , unique_designs[ i , 'type' ] , ' SET dom_wgt = ' , unique_designs[ i , 'fpc2' ] ) )

			DBI::dbSendQuery( db , paste0( 'ALTER TABLE c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_' , unique_designs[ i , 'type' ] , ' DROP COLUMN ' , unique_designs[ i , 'fpc2' ] ) )


			b.fields <- dbListFields( db , 'c00_fam' )[ !( dbListFields( db , 'c00_fam' ) %in% dbListFields( db , 'c00_dom' ) ) ]

			semifinal.merge <-
				paste0(
					'create table c00_dom_fam as (SELECT a.* , b.' ,
					paste( b.fields , collapse = ', b.' ) ,
					' from c00_dom as a inner join c00_fam as b ON a.v0102 = b.v0102 AND a.v0300 = b.v0300) WITH DATA'
				)
				
			dbSendQuery( db , semifinal.merge )


			b.fields <- dbListFields( db , 'c00_pes' )[ !( dbListFields( db , 'c00_pes' ) %in% dbListFields( db , 'c00_dom_fam' ) ) ]

			final.merge <-
				paste0(
					'create table c00 as (SELECT a.* , b.' ,
					paste( b.fields , collapse = ', b.' ) ,
					' from c00_dom_fam as a inner join c00_pes as b ON a.v0102 = b.v0102 AND a.v0300 = b.v0300 AND a.v0404 = b.v0404 ) WITH DATA'
				)
				
			dbSendQuery( db , final.merge )

			# now remove the dom + fam table,
			# since that's not of much use
			dbRemoveTable( db , 'c00_dom_fam' )

			# add columns named 'one' to each table..
			dbSendQuery( db , 'alter table c00_dom add column one int' )
			dbSendQuery( db , 'alter table c00_pes add column one int' )
			dbSendQuery( db , 'alter table c00_fam add column one int' )
			dbSendQuery( db , 'alter table c00 add column one int' )

			# ..and fill them all with the number 1.
			dbSendQuery( db , 'UPDATE c00_dom SET one = 1' )
			dbSendQuery( db , 'UPDATE c00_pes SET one = 1' )
			dbSendQuery( db , 'UPDATE c00_fam SET one = 1' )
			dbSendQuery( db , 'UPDATE c00 SET one = 1' )


			# now the current database contains four more tables than it did before
				# c00_dom (household)
				# c00_fam (family)
				# c00_pes (person)
				# c00 (merged)

			# the current monet database should now contain
			# all of the newly-added tables (in addition to meta-data tables)
			print( dbListTables( db ) )		# print the tables stored in the current monet database to the screen


			# confirm that the merged file has the same number of records as the person file
			stopifnot( 
				dbGetQuery( db , "select count(*) as count from c00_pes" ) == 
				dbGetQuery( db , "select count(*) as count from c00" )
			)


			#################################################
			# create a complex sample design object

			# save a person-representative design of the 2000 censo
			# warning: this command requires a long time, leave your computer on overnight.

			bw_dom_00 <- 
				bootweights( 
					dbGetQuery( db , "SELECT areap FROM c00_dom" )[ , 1 ] ,
					dbGetQuery( db , "SELECT v0300 FROM c00_dom" )[ , 1 ] ,
					replicates = 80 ,
					fpc = dbGetQuery( db , "SELECT dom_fpc FROM c00_dom" )[ , 1 ]
				)

			dom.design <-
				svrepdesign(
					weight = ~dom_wgt ,
					repweights = bw_dom_00$repweights ,
					combined.weights = FALSE ,
					scale = bw_dom_00$scale ,
					rscales = bw_dom_00$rscales ,
					data = 'c00_dom' ,
					dbtype = "MonetDBLite" ,
					dbname = dbfolder
				)

		
		
		
		
		
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )
		
		}
		
		invisible( TRUE )

	}

	

# define a special function to   #
# remove alphanumeric characters #
# from any data files that have  #
# been downloaded from ibge      #
ranc_censo <- 
	function( infiles , width ){

		tf_a <- tempfile()

		outcon <- file( tf_a , "w" )

		# if there are multiple infiles,
		# loop through them all!
		for ( infile in infiles ){

			incon <- file( infile , "r")

			line.num <- 0
			
			while( length( line <- readLines( incon , 1 , skipNul = TRUE ) ) > 0 ){

				# add blank spaces on the right side where they're absent.
				line <- stringr::str_pad( line , width , side = "right" , pad = " " )
				
				# save the file on the disk, so long as there's no weird corruption
				if( !is.na( iconv( line , "" , "ASCII" ) ) ) writeLines( line , outcon )
				# like line 509,451 and 2,575,789 of the combined 2000 sao paulo file.

				# add to the line counter #
				line.num <- line.num + 1

			}

			close( incon )
		}

		close( outcon )

		tf_a
	}
