%% Same logic as in jerr.erl is used here
%% 
-module(epgsql_pool_log).

-export([
    log_info/2,
    log_info/3,
    log_error/2,
    log_error/3,
    log_warning/2,
    log_warning/3,
    log_debug/2,
    log_debug/3
]).

new_format(Area, Format) -> 
    "[" ++ atom_to_list(Area) ++ "] " ++ Format ++ "~n".

log_info(Area, Format) -> log_info(Area, Format, []).
log_info(Area, Format, Args) ->
    error_logger:info_msg(new_format(Area, Format), Args).

log_error(Area, Format) -> log_error(Area, Format, []).
log_error(Area, Format, Args) ->
    error_logger:error_msg(new_format(Area, Format), Args).

log_warning(Area, Format) -> log_warning(Area, Format, []).
log_warning(Area, Format, Args) ->
    error_logger:warning_msg(new_format(Area, Format), Args).

log_debug(Area, Format) -> log_debug(Area, Format, []).
log_debug(Area, Format, Args) ->
    error_logger:ingo_msg("DEBUG: " ++ new_format(Area, Format), Args);
