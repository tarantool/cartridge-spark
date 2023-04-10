cartridge = require('cartridge')
replicasets = {{
    alias = 'app-router',
    roles = {'vshard-router', 'app.roles.api_router'},
    join_servers = {{uri = '0.0.0.0:3361'}}
}, {
    alias = 'app-router-second',
    roles = {'vshard-router', 'app.roles.api_router'},
    join_servers = {{uri = '0.0.0.0:3371'}}
}, {
    alias = 's1-storage',
    roles = {'vshard-storage', 'app.roles.api_storage'},
    join_servers = {{uri = 'localhost:3362'}, {uri = 'localhost:3363'}}
}, {
    alias = 's2-storage',
    roles = {'vshard-storage', 'app.roles.api_storage'},
    join_servers = {{uri = 'localhost:3364'}, {uri = 'localhost:3365'}}
}}
return cartridge.admin_edit_topology({replicasets = replicasets})
