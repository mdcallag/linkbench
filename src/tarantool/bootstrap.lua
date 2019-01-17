--value is taken from LinkStore for usage in getLinkList
local DEFAULT_LIMIT = 10000

local engine='vinyl'
local type_idx = 'tree'
local threshold_time_warn = 1
local coef_batch_select_limit = 4
local cycle_count_threshhold = 5
-- Autoincrement start value
local counter_node = 0

linkbench = {}

local function schema()
    --
    -- links
    --
    local links = box.schema.space.create('links',
        {id = 513, engine = engine})
    links:create_index('primary', {id = 0, unique = true, type = type_idx,
        parts={1, 'unsigned', 2, 'unsigned', 3, 'unsigned'}})

    links:create_index('id1_type_sh', {id = 1, unique = false, type='tree',
        parts={1, 'unsigned', 3, 'unsigned' }})
    links:create_index('id1_type_vis', {id = 2, unique = false, type='tree',
        parts={1, 'unsigned', 3, 'unsigned', 4, 'scalar'}})
    links:create_index('time', {id = 4, unique = false, type='tree',
        parts={6, 'unsigned'}})

    links:format{
        {name='id1',type='unsigned'},
        {name='id2',type='unsigned'},
        {name='link_type',type='unsigned'},
        {name='visibility',type='boolean'},
        {name='data', type = 'string'},
        {name='time', type = 'unsigned'},
        {name='version', type = 'unsigned'}
    }

    --
    -- nodes
    --
    local nodes = box.schema.space.create('nodes', {id=514, engine=engine})
    nodes:create_index('primary', {type='tree', parts={1, 'unsigned'}})

    box.space.nodes:format{
        {name='id', type='unsigned'},
        {name='type', type='unsigned'},
        {name='version', type='unsigned'},
        {name='time', type='unsigned'},
        {name='data', type='string'}
    }

    --
    -- counts
    --
    local counts = box.schema.space.create('counts',
        {id=515, engine=engine})
    counts:create_index('primary', {type=type_idx,
        parts={1, 'unsigned', 2, 'unsigned'}})

    box.space.counts:format{
        {name='id', type='unsigned'},
        {name='link_type', type='unsigned'},
        {name='count', type='unsigned'},
        {name='version', type='unsigned'},
        {name='time', type='unsigned'}
    }

    box.schema.user.create('linkbench', {password = 'linkbench'})

    local functions = {
        {"insert_link"};
        {"insert_links"};
        {"delete_link"};
        {"get_link"};
        {"multi_get_link"};
        {"get_link_list"};
        {"get_link_list_time", { language = "C" }};
        {"count_links"};
        {"add_counts"};
        {"add_bulk_nodes"};
        {"get_node"};
        {"update_node"};
        {"delete_node"};
    }

    for _, func in ipairs(functions) do
        box.schema.func.create("linkbench."..func[1], func[2])
        box.schema.user.grant('linkbench', 'execute', 'function',
            "linkbench."..func[1])
    end

    box.schema.user.grant('linkbench', 'read,write', 'space', 'links')
    box.schema.user.grant('linkbench', 'read,write', 'space', 'nodes')
    box.schema.user.grant('linkbench', 'read,write', 'space', 'counts')
end

box.once("linkbench:0.1", schema)

function linkbench.insert_links(list_of_objects)
    local links = box.space.links
    --box.begin()
    for i=1,#list_of_objects,1 do
        box.begin()
        links:replace(list_of_objects[i])
        box.commit()
    end
    --box.commit()
end

function linkbench.insert_link(object)
    local links = box.space.links
    local counts = box.space.counts

    box.begin()

    links:replace(object)

    local count_t = counts:get{object[1], object[3]}
    local current_time = os.time()

    local to_replace =  {object[1], object[3], 1 , 0, current_time}
    if (object[4]) then
        if (count_t) then
            to_replace[3] = count_t[3] + 1
            to_replace[4] = count_t[4] + 1
        end
    else
        if (count_t) then
            to_replace[3] = count_t[3] - 1
            to_replace[4] = count_t[4] + 1
        end
    end

    counts:replace(to_replace)

    box.commit()
end

function linkbench.delete_link(index, force)
    local links = box.space.links
    local counts = box.space.counts

    box.begin()
    local tuple = box.space.links:get(index)

    if (tuple) then
        local current_time = os.time()
        local visibility = tuple[4]
        if (force) then
            links:delete(index)
        end

        if (visibility) then
            local counts_t = counts:get{index[1], index[3]}
            if (counts_t) then
                local new_count = 0
                if (counts_t[3] > 0) then
                    new_count = counts_t[3] - 1
                end
                counts:update({index[1], index[3]},
                    {{'=', 3, new_count}, {'=', 5, current_time}})
            end
        end
        if (visibility and not force) then
            links:update(index, {{'=', 4, false}})
        end
    end

    box.commit()
end

function linkbench.get_link(ind)
    return box.space.links:get{ind[1], ind[2], ind[3]}
end

function linkbench.multi_get_link(id1, link_type, id2s)
    local selected = box.space.links.index.id1_type_sh:select{id1, link_type}
    local set = {}
    for _,tuple in pairs(selected) do
        set[tuple[2]] = tuple
    end
    local result = {}
    for k, id2 in pairs(id2s) do
        result[k] = set[id2]
    end

    return result
end

function linkbench.get_link_list(id1, type_link)
    return box.space.links.index.id1_type_vis:
        select({id1, type_link, true}, { limit=DEFAULT_LIMIT })
end

function linkbench.count_links(id1, type_link)
    local tuple = box.space.counts:select{id1, type_link}
    if (tuple[1]) then
        return tuple[1][3]
    else
        return nil
    end
end

function linkbench.add_counts(list_of_objects)
    local counts = box.space.counts
    for _, object in pairs(list_of_objects) do
        counts:replace(object)
    end
end

function linkbench.add_bulk_nodes(list_of_objects)
    local nodes = box.space.nodes
    local result = {}

    box.begin()
    for i, object in pairs(list_of_objects) do
        counter_node = counter_node  + 1
        object[1] = counter_node
        nodes:replace(object)
        result[i] = counter_node
    end

    box.commit()
    return result
end

function linkbench.get_node(id, type_)
    local nodes = box.space.nodes

    local node = nodes:get{id}
    if (not node) then
        return nil
    end

    if (node[2] ~= type_) then
        return nil
    else
        return node
    end
end

function linkbench.update_node(node)
    local nodes = box.space.nodes

    box.begin()

    local res = false
    local check = nodes:get{node[1]}
    if (check) then
        nodes:replace(node)
        res = true
    end

    box.commit()
    return res
end

function linkbench.delete_node(id, type_)
    local nodes = box.space.nodes

    box.begin()

    local node = nodes:get{id}
    local res = true
    if (not node or node[2] ~= type_) then
        res = false
    else
        nodes:delete{id}
    end

    box.commit()

    return res
end
