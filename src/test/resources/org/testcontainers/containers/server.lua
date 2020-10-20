box.cfg {
    listen = 3301,
    memtx_memory = 128 * 1024 * 1024, -- 128 Mb
    -- log = 'file:/tmp/tarantool.log',
    log_level = 6,
}
-- API user will be able to login with this password
box.schema.user.create('api_user', { password = 'secret' })
-- API user will be able to create spaces, add or remove data, execute functions
box.schema.user.grant('api_user', 'read,write,execute', 'universe')

s = box.schema.space.create('_spark_test_space')

s:format({
    {name = 'id', type = 'unsigned'},
    {name = 'name', type = 'string'},
    {name = 'value', type = 'unsigned'}
})

s:create_index('primary', {type = 'tree', parts = {'id'}})

for i=1,100 do
    s:insert{i, 'a'..i, i * 100}
end