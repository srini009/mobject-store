/*
 * (C) 2017 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <stdlib.h>
#include "read-responses.h"
#include "omap-iter-impl.h"
#include "utlist.h"

typedef rd_response_base_t (*build_matching_fn)(rd_action_base_t);

static rd_response_base_t build_matching_stat(rd_action_stat_t a);
static rd_response_base_t build_matching_read(rd_action_read_t a);
static rd_response_base_t build_matching_omap_get_keys(rd_action_omap_get_keys_t a);
static rd_response_base_t build_matching_omap_get_vals(rd_action_omap_get_vals_t a);
static rd_response_base_t build_matching_omap_get_vals_by_keys(rd_action_omap_get_vals_by_keys_t a);

typedef void (*free_response_fn)(rd_response_base_t);

static void free_resp_omap(rd_response_omap_t a) {
	omap_iter_free(a->iter);
	free(a);
};

static build_matching_fn match_fn[] = {
	NULL,
	(build_matching_fn)build_matching_stat,
	(build_matching_fn)build_matching_read,
	(build_matching_fn)build_matching_omap_get_keys,
	(build_matching_fn)build_matching_omap_get_vals,
	(build_matching_fn)build_matching_omap_get_vals_by_keys
};

static free_response_fn free_fn[] = {
	NULL,
	(free_response_fn)free,
	(free_response_fn)free,
	(free_response_fn)free_resp_omap
};

rd_response_base_t build_matching_read_responses(rd_action_base_t actions)
{
	rd_action_base_t a;
	rd_response_base_t resp = NULL;
	rd_response_base_t tmp = NULL;

	DL_FOREACH(actions, a) {
		tmp = match_fn[a->type](a);
		DL_APPEND(resp, tmp);
	}

	return resp;
}

void free_read_responses(rd_response_base_t responses)
{
	rd_response_base_t r, tmp;
	DL_FOREACH_SAFE(responses, r, tmp) {
		DL_DELETE(responses, r);
		free_fn[r->type](r);
	}
}

rd_response_base_t build_matching_stat(rd_action_stat_t a)
{
	rd_response_stat_t resp = (rd_response_stat_t)calloc(1, sizeof(*resp));
	resp->base.type = READ_RESPCODE_STAT;
	a->psize        = &(resp->psize);
	a->pmtime       = &(resp->pmtime);
	a->prval        = &(resp->prval);
	return (rd_response_base_t)resp;
}

rd_response_base_t build_matching_read(rd_action_read_t a)
{
	rd_response_read_t resp = (rd_response_read_t)calloc(1, sizeof(*resp));
	resp->base.type = READ_RESPCODE_READ;
	a->bytes_read   = &(resp->bytes_read);
	a->prval        = &(resp->prval);
	return (rd_response_base_t)resp;
}

rd_response_base_t build_matching_omap_get_keys(rd_action_omap_get_keys_t a)
{
	rd_response_omap_t resp = (rd_response_omap_t)calloc(1, sizeof(*resp));
	resp->base.type = READ_RESPCODE_OMAP;
	a->prval = &(resp->prval);
	a->iter  = &(resp->iter);
	return (rd_response_base_t)resp;
}

rd_response_base_t build_matching_omap_get_vals(rd_action_omap_get_vals_t a)
{
	rd_response_omap_t resp = (rd_response_omap_t)calloc(1, sizeof(*resp));
	resp->base.type = READ_RESPCODE_OMAP;
	a->prval = &(resp->prval);
	a->iter  = &(resp->iter);
	return (rd_response_base_t)resp;
}

rd_response_base_t build_matching_omap_get_vals_by_keys(rd_action_omap_get_vals_by_keys_t a)
{
	rd_response_omap_t resp = (rd_response_omap_t)calloc(1, sizeof(*resp));
	resp->base.type = READ_RESPCODE_OMAP;
	a->prval = &(resp->prval);
	a->iter  = &(resp->iter);
	return (rd_response_base_t)resp;
}
