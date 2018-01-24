get_catalog_censoescolar <-
  function( data_name = "censoescolar" , output_dir , ... ){

	if ( !requireNamespace( "archive" , quietly = TRUE ) ) stop( "archive needed for this function to work. to install it, type `devtools::install_github( 'jimhester/archive' )`" , call. = FALSE )

    inep_portal <- "http://portal.inep.gov.br/web/guest/microdados"

    inep_html <- xml2::read_html(inep_portal)
    w <- rvest::html_attr( rvest::html_nodes( inep_html , "a" ) , "href" )

    these_links <- grep( "censo_escolar(.*)zip$" , w , value = TRUE , ignore.case = TRUE )

    censoescolar_years <- substr( gsub( "[^0-9]" , "" , these_links ) , 1 , 4 )

    catalog <-
      data.frame(
        year = as.numeric( censoescolar_years ) ,
        dbfile = paste0( output_dir , "/SQLite.db" ) ,
        output_folder = paste0( output_dir , "/" , censoescolar_years ) ,
        full_url = these_links ,
        stringsAsFactors = FALSE
      )


    # sort by year
    catalog[ order( catalog$year ) , ]

  }


lodown_censoescolar <-
  function( data_name = "censoescolar" , catalog , ... ){

	on.exit( print( catalog ) )

    tf <- tempfile()

    for ( i in seq_len( nrow( catalog ) ) ){

      # open the connection to the monetdblite database
      db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

      # download the file
      cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , method = ifelse( .Platform$OS.type == "unix" , "wget" , "auto" ) , quiet = TRUE )
      unzipped_files <- custom_extract( tf , ext_dir = catalog[ i , "output_folder" ] )

      if( .Platform$OS.type != 'windows' ){
        sapply(unique(dirname(gsub( "\\\\" ,  "/" , unzipped_files ))),dir.create,showWarnings=FALSE)
        file.rename( unzipped_files , gsub( "\\\\" ,  "/" , unzipped_files ) )
        unzipped_files <- gsub( "\\\\" ,  "/" , unzipped_files )
      }

      if( catalog[ i , 'year' ] <= 2006 ){

        for ( zipfile in grep( "\\.zip$" , unzipped_files , ignore.case = TRUE , value = TRUE ) ) { unzipped_files <- unique( c( unzipped_files , custom_extract( zipfile = zipfile , ext_dir = np_dirname( zipfile ) , rm.main = TRUE ) ) ) }

        sas_files <- grep( "\\.sas$", unzipped_files, value = TRUE , ignore.case = TRUE )

        sas_scaledowns <- gsub( "SAS|_" , "" , gsub( "\\.sas|\\.SAS" , "" , gsub( paste0( "INPUT|" , catalog[ i , 'year' ] ) , "" , basename( sas_files ) ) ) )
        sas_scaledowns <- ifelse( grepl( "ESC" , sas_scaledowns ) & !grepl( "INDICESC" , sas_scaledowns ) , gsub( "ESC" , "" , sas_scaledowns ) , sas_scaledowns )

        datafile_matches <- lapply( sas_scaledowns , function( z ) unzipped_files[ grepl( ifelse( grepl( "DESPORTO" , z , ignore.case = TRUE ) , "DESP" , z ) , basename( unzipped_files ) , ignore.case = TRUE ) & grepl( "dados" , unzipped_files , ignore.case = TRUE ) ] )
        datafile_matches <- lapply( datafile_matches , function( z ) z[ !grepl( "\\.zip" , z , ignore.case = TRUE ) ] )

        these_tables <-
          data.frame(
            sas_script = sas_files ,
            data_file = unique( unlist( datafile_matches ) ) ,
            db_tablename = paste0( tolower( sas_scaledowns ) , "_" , catalog[ i , 'year' ] ) ,
            stringsAsFactors = FALSE
          )

        for ( j in seq( nrow( these_tables ) ) ){

          this_sas <- file( these_tables[ j , 'sas_script' ] , 'r' , encoding = 'windows-1252' )

          # write the file to the disk
          w <- custom_strip_special( readLines( this_sas ) )

          close( this_sas )

          # remove all tab characters
          w <- gsub( '\t' , ' ' , w )

          w <- gsub( '@ ' , '@' , w , fixed = TRUE )
          w <- gsub( "@371 CEST_SAUDE  " , "@371 CEST_SAUDE $1 " , w , fixed = TRUE )
          w <- gsub( "@379 OUTROS  " , "@379 OUTROS $1 " , w , fixed = TRUE )
          w <- gsub( "VEF918 11" , "VEF918 11" , w , fixed = TRUE )
          w <- gsub( "VEE1411( +)/" , "VEE1411 7. /" , w )
          w <- gsub( "VEE1412( +)/" , "VEE1412 7. /" , w )

          # overwrite the file on the disk with the newly de-tabbed text
          file.remove( these_tables[ j , 'sas_script' ] )
          writeLines( w , these_tables[ j , 'sas_script' ] )

          x <- read_SAScii( these_tables[ j , 'data_file' ] , these_tables[ j , 'sas_script' ] , na_values = c( "" , "." ) )
          file.remove( these_tables[ j , 'data_file' ] )

          # convert column names to lowercase
          names( x ) <- tolower( names( x ) )

          DBI::dbWriteTable( db , these_tables[ j , 'db_tablename' ] , x )

          rm( x )

          cat( tolower( gsub( "\\..*" , "" , basename( these_tables[ j , 'data_file' ] ) ) ) , "stored at" ,
               these_tables[ j , 'db_tablename' ] , "\r\n\n" )

          catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , DBI::dbGetQuery( db , paste( "SELECT COUNT(*) FROM" , these_tables[ j , 'db_tablename' ] ) )[ 1 , 1 ] , na.rm = TRUE )

        }


      } else {

        this_excel_file <- unzipped_files [ grep( "^ANEXO I -" , basename( unzipped_files ), value = FALSE, ignore.case = TRUE ) ]
        data_files <- grep( "\\.rar$|\\.csv$", unzipped_files, value = TRUE , ignore.case = TRUE )

        for ( this_table_type in c( "docente" , "matricula" , "turma" , "escola" ) ) {

          tabelas <- data_files[ grep( this_table_type , basename( data_files ), value = FALSE, ignore.case = TRUE ) ]

          for ( j in seq_along( tabelas ) ) {

            codebook <- read_excel_metadata( this_excel_file , this_table_type )

            if ( grepl( "\\.rar$" , tabelas[ j ] , ignore.case = TRUE ) ) {

              this_data_file <- custom_extract( normalizePath( tabelas[ j ] ) , ext_dir = np_dirname( normalizePath( tabelas[ j ] ) ) , rm.main = TRUE )

            } else {

              this_data_file <- tabelas[ j ]

            }

            columns.in.datafile <- strsplit( readLines( this_data_file , n = 1 ) , "\\|" )[ 1 ] [[ 1 ]]
            columns.in.datafile <- tolower( columns.in.datafile )
            codebook <- codebook[ codebook$`Nome da Variavel` %in% columns.in.datafile , ]
            codebook <- codebook[ match( columns.in.datafile , codebook$`Nome da Variavel` ) , ]

            # force type
            codebook [ codebook$`Nome da Variavel` %in% "id_didatica_metodologia" , "Tipo" ] <- "character"

            if ( this_table_type == "matricula" ) {

              suppressMessages(
                DBI::dbWriteTable(
                  db,
                  paste0( this_table_type , "_" , catalog[ i , "year" ] ) ,
                  this_data_file ,
                  sep = "|" ,
                  lower.case.names = TRUE ,
                  append = ( paste0( this_table_type , "_" , catalog[ i , "year" ] ) %in% DBI::dbListTables( db ) ) ,
                  #nrow.check = 70000
                  colClasses = ifelse( grepl( "num" , codebook$Tipo , ignore.case = TRUE ) , "numeric" , "character" )
                ) )

              file.remove( this_data_file )

            } else {

              suppressMessages(
                tryCatch(
                  DBI::dbWriteTable(
                    db,
                    ifelse( length( tabelas ) > 1 , tolower( gsub( "\\..*" , "" , basename( this_data_file ) ) ) , paste0( this_table_type , "_" , catalog[ i , "year" ] ) ),
                    this_data_file ,
                    sep = "|" ,
                    lower.case.names = TRUE ,
                    overwrite = TRUE ,
                    temporary = ifelse( length( tabelas ) > 1 , FALSE , TRUE ) ,
                    colClasses = ifelse( grepl( "num" , codebook$Tipo , ignore.case = TRUE ) , "numeric" , "character" )
                  ) , error = function(e) {
                    cat( "removing non-utf8 characters\r" )
                    cleaned_data_file <- remove_nonutf8_censoescolar( this_data_file ) ;
                    db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )
                    DBI::dbWriteTable(
                      db,
                      ifelse( length( tabelas ) > 1 , tolower( gsub( "\\..*" , "" , basename( this_data_file ) ) ) , paste0( this_table_type , "_" , catalog[ i , "year" ] ) ),
                      cleaned_data_file ,
                      sep = "|" ,
                      lower.case.names = TRUE ,
                      overwrite = TRUE ,
                      temporary = ifelse( length( tabelas ) > 1 , FALSE , TRUE ) ,
                      colClasses = ifelse( grepl( "num" , codebook$Tipo , ignore.case = TRUE ) , "numeric" , "character" )
                    )
                    file.remove( cleaned_data_file )
                  } )
              )

              file.remove( this_data_file )

              if ( j == length( tabelas ) & length( tabelas ) > 1 ) {

                db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )

                field.names <- data.frame( tablename = NULL , field = NULL )
                for ( tabela in tolower( gsub( "\\..*" , "" , basename( tabelas ) ) ) ) {
                  field.names <- rbind( field.names , data.frame( tablename = tabela , field = DBI::dbListFields( db , tabela ) , stringsAsFactors = FALSE ) )
                }

                field.names <- reshape2::dcast( field.names , field ~ tablename , value.var = "field" )

                std.string <- sapply( seq_along( tabelas ) , FUN = function( x ) {
                  paste0( "SELECT ",
                          paste0( ifelse( is.na( field.names[ , x + 1 ] ) , paste( "NULL AS" , field.names[ , 1 ] ) , field.names[ , 1 ] ) , collapse = " , " ),
                          " FROM ",  colnames( field.names ) [ x + 1 ] )
                }  )

                create.stacked <- paste0( "CREATE TABLE " , paste0( this_table_type , "_" , catalog[ i , "year" ] ) , " AS ( " ,
                                          paste0( std.string , collapse = " ) UNION ALL ( " ) ,
                                          " )" )

                DBI::dbSendQuery( db , create.stacked )

                for ( tabela in colnames( field.names )[ -1 ] ) DBI::dbRemoveTable( db , tabela )

              }

            }

            cat( tolower( gsub( "\\..*" , "" , basename( this_data_file ) ) ), "stored at" , paste0( this_table_type , "_" , catalog[ i , "year" ] ) , "\r\n\r" )

          }

        }

        db <- DBI::dbConnect( RSQLite::SQLite() , catalog[ i , 'dbfile' ] )
        catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM matricula_" , catalog[ i , "year" ] ) )[ 1 , 1 ]

      }

      # delete the temporary files?  or move some docs to a save folder?
      suppressWarnings( file.remove( tf ) )

      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfile' ] , "'\r\n\n" ) )

    }

	on.exit()

    catalog

  }



remove_nonutf8_censoescolar <-
  function( infile ){

    tf_a <- tempfile()

    outcon <- file( tf_a , "w" )

    incon <- file( infile , "r" )

    while( length( line <- readLines( incon , 1 , warn = FALSE ) ) > 0 ) writeLines( custom_strip_special( line ) , outcon )

    close( incon )

    close( outcon )

    tf_a
  }

read_excel_metadata <- function( metadata_file , table_type ) {

  quietly_sheets <- purrr::quietly(readxl::excel_sheets)
  quietly_readxl <- purrr::quietly(readxl::read_excel)

  sheet_name <- grep( table_type, custom_strip_special( quietly_sheets( metadata_file )$result ) , ignore.case = TRUE )

  codebook <-  data.frame( quietly_readxl( metadata_file , sheet = sheet_name , skip = 0 )$result , stringsAsFactors = FALSE )
  codebook <- codebook [ which( codebook[ , 1 ] %in% c("N" , "ORD" ) ): nrow(codebook) , ]
  colnames( codebook ) <- codebook[ 1 , ]
  codebook <- codebook[ -1 , ]
  codebook <- codebook[ !is.na( suppressWarnings( as.integer( codebook[ , 1 ] ) ) ), ]
  colnames( codebook ) <- custom_strip_special( colnames(codebook) )
  codebook <- tryCatch(
    codebook <- codebook[ !is.na( codebook[ , 2 ] ) , c( "Nome da Variavel" , "Tipo" , "Tam.(1)" ) ] ,
    error = function( e ) {

      possible.names <- list( c( "Nome da Variavel" , "Tipo" , "Tam. (1)" ) , c( "Nome novo da Variavel" , "Tipo" , "Tam. (1)" ), c( "Nome novo da Variavel" , "Tipo" , "Tam.(1)" ) , c( "NOME DA VARIAVEL" , "TIPO" , "TAMANHO" ) )

      for( name_set in possible.names ) {

        if ( all( name_set %in% colnames( codebook ) ) ) {

          codebook <- codebook[ !is.na( codebook[ , 2 ] ) , name_set ]
          colnames( codebook ) <- c( "Nome da Variavel" , "Tipo" , "Tam.(1)" )

          return( codebook )

        }

      }

    } )
  codebook$`Nome da Variavel` <- tolower( codebook$`Nome da Variavel` )
  codebook[ is.na( codebook$Tipo ) , "Tipo" ] <- "Char"
  codebook[ is.na( codebook$Tipo ) , "Tam.(1)" ] <- 4
  codebook[ grepl( "^co_curso_[1-9]" , codebook$`Nome da Variavel` ) , c( "Tipo" ) ] <- "Char"
  codebook[ grepl( "^co_curso_[1-9]" , codebook$`Nome da Variavel` ) , c( "Tam.(1)" ) ] <- 10
  codebook[ grepl( "^dt_" , codebook$`Nome da Variavel` ) , c( "Tipo" ) ] <- "Char"
  codebook[ grepl( "^dt_" , codebook$`Nome da Variavel` ) , c( "Tam.(1)" ) ] <- 10

  codebook

}


custom_extract <- function( zipfile , ext_dir , rm.main = FALSE ) {

  # td <- file.path( tempdir() , "temp_unzip" )
  td <- ifelse( .Platform$OS.type == 'windows' , file.path( tempdir() , "temp_unzip" ) , normalizePath( "~/temp_unzip" , mustWork = FALSE ) )

  archive::archive_extract( zipfile , dir = td )

  new_files <- list.files( td , recursive = TRUE , full.names = FALSE )

  for ( expath in unique( np_dirname( paste0( ext_dir , "/" , new_files ) ) ) ) {
    if ( !dir.exists( expath ) ) dir.create( expath , showWarnings = FALSE , recursive = TRUE )
  }

  file.copy( from = file.path( td , new_files ) , to = file.path( ext_dir , new_files ) )

  unlink( td , recursive = TRUE , force = TRUE )

  if ( rm.main ) { file.remove( zipfile ) }

  return( normalizePath( ( file.path( ext_dir , new_files ) ) , mustWork = FALSE ) )

}

custom_strip_special <- function(x) {
  y <- iconv( x , from = ifelse( .Platform$OS.type == 'windows' , "utf8" , "" ) , to = "ASCII//TRANSLIT" , sub = " " )
  y <- gsub( "'|~" , "" , y  )
  y
}
