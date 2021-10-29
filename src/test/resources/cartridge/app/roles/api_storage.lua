local function init_space()
    -- Book space
    local test_space = box.schema.space.create(
        'test_space',
        {
            format = {
                { name = 'id', type = 'unsigned' },
                { name = 'bucket_id', type = 'unsigned' },
                { name = 'unique_key', type = 'string' },
                { name = 'book_name', type = 'string' },
                { name = 'author', type = 'string' },
                { name = 'year', type = 'unsigned', is_nullable = true },
                { name = 'issuerAddress', type = 'any', is_nullable = true },
                { name = 'storeAddresses', type = 'array', is_nullable = true },
                { name = 'readers', type = 'array', is_nullable = true },
                { name = 'issueDate', type = 'string', is_nullable = true },
            },
            if_not_exists = true,
        }
    )

    test_space:create_index('id', {
        parts = { 'id' },
        if_not_exists = true,
    })

    test_space:create_index('inx_author', {
        type = 'tree',
        unique = false,
        parts = { 'author' },
        if_not_exists = true,
    })

    test_space:create_index('bucket_id', {
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = true,
    })

    test_space:create_index('name', {
        type = 'tree',
        parts = { 'book_name' },
        unique = true,
        if_not_exists = true,
    })

    local orders = box.schema.space.create(
        'orders',
        {
            format = {
                { name = 'id', type = 'unsigned' },
                { name = 'bucket_id', type = 'unsigned' },
                { name = 'order_type', type = 'string' },
                { name = 'order_value', type = 'number' },
                { name = 'order_items', type = 'array' },
                { name = 'options', type = 'map' },
                { name = 'cleared', type = 'boolean' },
            },
            if_not_exists = true,
        }
    )

    orders:create_index('id', {
        parts = { 'id' },
        if_not_exists = true,
    })

    orders:create_index('bucket_id', {
        parts = { 'bucket_id' },
        unique = false,
        if_not_exists = true,
    })
end

local function init(opts)
    if opts.is_master then
        init_space()
    end

    rawset(_G, 'ddl', { get_schema = require('ddl').get_schema })

    return true
end

return {
    role_name = 'app.roles.api_storage',
    init = init,
    dependencies = {
        'cartridge.roles.crud-storage'
    }
}
