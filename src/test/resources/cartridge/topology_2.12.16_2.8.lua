cartridge = require('cartridge')
replicasets = {{
    alias = 'app-router',
    roles = {'vshard-router', 'app.roles.api_router'},
    join_servers = {{uri = '0.0.0.0:3331'}}
}, {
    alias = 'app-router-second',
    roles = {'vshard-router', 'app.roles.api_router'},
    join_servers = {{uri = '0.0.0.0:3341'}}
}, {
    alias = 's1-storage',
    roles = {'vshard-storage', 'app.roles.api_storage'},
    join_servers = {{uri = 'localhost:3332'}, {uri = 'localhost:3333'}}
}, {
    alias = 's2-storage',
    roles = {'vshard-storage', 'app.roles.api_storage'},
    join_servers = {{uri = 'localhost:3334'}, {uri = 'localhost:3335'}}
}}
return cartridge.admin_edit_topology({replicasets = replicasets})
