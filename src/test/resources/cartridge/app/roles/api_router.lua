local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

local function get_schema()
	for _, instance_uri in pairs(cartridge_rpc.get_candidates('app.roles.api_storage', { leader_only = true })) do
		local conn = cartridge_pool.connect(instance_uri)
		return conn:call('ddl.get_schema', {})
	end
end

local function init(opts)

    rawset(_G, 'ddl', { get_schema = get_schema })

    return true
end

return {
    role_name = 'app.roles.api_router',
    init = init,
    dependencies = {
        'cartridge.roles.crud-router',
    }
}
