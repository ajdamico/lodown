#' @rdname lodown
#' @export
#'
get_catalog <-
  function( data_name , output_dir = getwd() , ... ){

    cat_fun <- getFromNamespace( paste0( "get_catalog_" , data_name ) , "lodown" )

    cat_fun( output_dir = output_dir , ... )

  }
