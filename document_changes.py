import dictdiffer
from collections import defaultdict

non_tracked_fields = ['updated_datetime', 'created_datetime', "person_id", "auth0_user_id"]


def get_dict_diff_grp(previous_dict, current_dict):
    """Block to get difference fields dict.
    Args:
        previous_dict: cleaned previous_dict data (dict)
        current_dict: cleaned profile current state data (dict)
    Returns:
        dict_diff_grp: difference dict grouped by field name(dict)
    """
    dict_diff = list(dictdiffer.diff(previous_dict, current_dict))
    dict_diff_grp = {}
    try:
        for elem in dict_diff:
            if elem[0] == 'change' and isinstance(elem[1],str):
                if elem[1].split('.')[0] in dict_diff_grp.keys():
                    dict_diff_grp[elem[1].split('.')[0]].append(elem)
                else:
                    dict_diff_grp[elem[1].split('.')[0]] = [elem]
            elif elem[0] == 'change' and isinstance(elem[1],list):
                if elem[1][0] in dict_diff_grp.keys():
                    dict_diff_grp[elem[1][0]].append(elem)
                else:
                    dict_diff_grp[elem[1][0]] = [elem]
            elif elem[0] == 'add' and isinstance(elem[1],str):
                for add_ele in elem[2]:
                    if add_ele[0] in dict_diff_grp.keys():
                        dict_diff_grp[add_ele[0]].append(add_ele)
                    elif add_ele[0] not in previous_dict.keys() and (elem[1].split('.')[0] in previous_dict.keys()):
                        dict_diff_grp[elem[1].split('.')[0]] = [add_ele]
                    elif add_ele[0] in previous_dict.keys() or add_ele[0] in current_dict.keys():
                        dict_diff_grp[add_ele[0]]= [add_ele]
            elif elem[0] == 'remove' and isinstance(elem[1],str):
              for add_ele in elem[2]:
                    if add_ele[0] in dict_diff_grp.keys():
                        dict_diff_grp[add_ele[0]].append(add_ele)
                    elif add_ele[0] not in current_dict.keys() and (elem[1].split('.')[0] in current_dict.keys()):
                        if ('.' in elem[1] and elem[1] not in dict_diff_grp.keys()) or (str(add_ele[0]).isdigit() and elem[1] not in dict_diff_grp.keys()):
                          dict_diff_grp[elem[1].split('.')[0]] = [add_ele]
                        elif str(add_ele[0]).isdigit() and elem[1] in dict_diff_grp.keys():
                          dict_diff_grp[elem[1].split('.')[0]].append(add_ele)
                        else:
                          dict_diff_grp[add_ele[0]]= [add_ele]
                    elif add_ele[0] in current_dict.keys() or add_ele[0] in previous_dict.keys():
                        dict_diff_grp[add_ele[0]]= [add_ele]
    except Exception as e:
        logger.error(f"Failed when prepare changes field dict_difference, Error: {str(e)}")
    return dict_diff_grp

def no_dicts(args):
    # args must be a list
    if isinstance(args, list):
        return not any(isinstance(obj, dict) for obj in args)
    else:
        raise(ValueError("give args as a list to no_list_or_dict(args)"))

def val_or_dict(val):
    if val:
        return val
    else:
        return {}

def no_lists_or_dicts(args):
    # args must be a list
    if isinstance(args, list):
        return not any(isinstance(obj, typ) for obj in args for typ in [list, dict])
    else:
        raise(ValueError("give args as a list to no_list_or_dict(args)"))

def validate_before_and_after(in_dict):
    nested_in = val_or_dict(in_dict.get("nested"))
    if not nested_in:
        return(None, None)
    before = val_or_dict(nested_in.get("before"))
    after = val_or_dict(nested_in.get("after"))
    return(before, after)


# recursive, so what goes in, must come out
def find_changes_rec(in_dict = {}):
    out_keys = ["added", "deleted", "changed", "nested"]
    before, after = validate_before_and_after(in_dict)
    # stopping conidition for recursion
    if not before and not after:
        return(None)
    # inner function for lists, could recurse from the outer function
    def find_changes_inner_lists(before, after):
        # stopping conidition for recursion
        if not before and not after:
            return(None)
        else:
            # compare any scalar values
            changed_k = defaultdict(list)
            scalars_before = set([b for b in before if no_lists_or_dicts([b])]) # list input even if only one arg
            scalars_after = set([a for a in after if no_lists_or_dicts([a])])
            if scalars_before!=scalars_after:
                added_scalars = list(scalars_after.difference(scalars_before))
                deleted_scalars = list(scalars_before.difference(scalars_after))
                if added_scalars:
                    changed_k["added"] += added_scalars
                if deleted_scalars:
                    changed_k["deleted"] += deleted_scalars
            # compare dictionaries by id
            nested_ids_before = [b["id"] for b in before if isinstance(b, dict) and b.get("id")]
            nested_ids_after = [a["id"] for a in after if isinstance(a, dict) and a.get("id")]
            nested_id_dicts_before = {b["id"] : b for b in before if isinstance(b, dict) and b.get("id")}
            nested_id_dicts_after = {a["id"] : a for a in after if isinstance(a, dict) and a.get("id")}
            # easy? stuff
            new_nested_ids = set(nested_ids_after).difference(nested_ids_before)
            del_nested_ids = set(nested_ids_before).difference(nested_ids_after)
            if new_nested_ids:
                changed_k["added"] += [nested_id_dicts_after[nni] for nni in new_nested_ids]
            if del_nested_ids:
                changed_k["deleted"] += [nested_id_dicts_before[dni] for dni in del_nested_ids]
            # comparing dicts with same id, ignore when dicts are same
            shared_nested_ids = set(nested_ids_after).intersection(nested_ids_before)
            shared_nested_ids_diff = [sni for sni in shared_nested_ids if nested_id_dicts_before[sni]!=nested_id_dicts_after[sni]]
            if shared_nested_ids_diff:
                # recurse the list of comparable dicts
                for snid in shared_nested_ids_diff:
                    changed_k["nested"].append(
                        { snid :
                            find_changes_rec(
                                {
                                    "nested" : {"before" : nested_id_dicts_before[snid], "after" : nested_id_dicts_after[snid]}
                                }
                            )
                        }
                    )
            # inner dictionaries without ids.
            nested_anon_dicts_before = [b for b in before if isinstance(b, dict) and not b.get("id")]
            nested_anon_dicts_after = [a for a in after if isinstance(a, dict) and not a.get("id")]
            # if there is only one, we can compare with recursion
            if len(nested_anon_dicts_before)==1 and len(nested_anon_dicts_after)==1 \
            and nested_anon_dicts_before[0]!=nested_anon_dicts_after[0]:
                changed_k["nested"].append(find_changes_rec(
                                            {
                                                "nested" : {"before" : nested_anon_dicts_before[0], "after" : nested_anon_dicts_after[0]}
                                            }
                                        )
                                    )
            else:
                if nested_anon_dicts_after:
                    changed_k["added"] += nested_anon_dicts_after
                if nested_anon_dicts_before:
                    changed_k["deleted"] += nested_anon_dicts_before
            # finally, recurse inner function for nested lists
            nested_lists_before = [b for b in before if isinstance(b, list)]
            nested_lists_after = [a for a in after if isinstance(a, list)]
            # if there is only one, we can compare with recursion
            if len(nested_lists_before)==1 and len(nested_lists_after)==1:
                if nested_lists_before[0]!=nested_lists_after[0]:
                    inner_lists_k = dict(find_changes_inner_lists(nested_lists_before[0], nested_lists_after[0])) # cast defaultdict to regular dict for json compatability
                    changed_k["nested"].append(inner_lists_k)
                # else they are the same. do nothing
            else: # the lengths are zero or greater than one
                if nested_lists_after:
                    changed_k["added"] += nested_lists_after
                if nested_lists_before:
                    changed_k["deleted"] += nested_lists_before
            return(changed_k)
    # initialise
    added = {}
    deleted = {}
    changed = {}
    nested_out = {}
    # easy stuff
    new_keys = set(after.keys()).difference(before.keys())
    del_keys = set(before.keys()).difference(after.keys())
    for k in new_keys:
        if k not in non_tracked_fields:
            added[k] = after[k]
    for k in del_keys:
        if k not in non_tracked_fields:
            deleted[k] = before[k]
    # all the rest
    shared_keys = set(after.keys()).intersection(before.keys())
    # for recursion, nested out must look like {"before" : some_dict, "after" : some_dict}
    # stopping if nested out is {}
    for k in shared_keys:
        # only if different
        if before[k]!=after[k] and k not in non_tracked_fields:
            # if different types or both scalar
            if type(before[k])!=type(after[k]) or no_lists_or_dicts([before[k], after[k]]):
                if str(before[k]) != str(after[k]):
                    changed[k] = {"before:"+ str(before[k]):"after:" +str(after[k])}
            else: # they are the same type, either list or dict
                # therefore, just check the type of the before item
                if isinstance(before[k], dict): # simplest case, recurse any dictionaries
                    if str(before[k]) != str(after[k]):
                        nested_out[k] = find_changes_rec({"nested" : {"before" : before[k], "after" : after[k]}})
                else: # it's a list
                    changed_k = find_changes_inner_lists(before[k], after[k])
                    # ready to add to outputs
                    if any(changed_k[o] for o in out_keys):
                        changed[k] = {}
                        for o in out_keys:
                            if changed_k[o]:
                                changed[k].update({o : changed_k[o]})
    out_dict = {}
    for k, v in zip(out_keys, [added, deleted, changed, nested_out]):
        if v:
            out_dict.update({k : v})
    return(out_dict)