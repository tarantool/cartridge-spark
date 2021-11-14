local crud = require('crud')

local function truncate_space(space)
    local ok, err
    ok, err = crud.truncate(space)
    if (not ok) then
        error("Failed to truncate space '" .. space .. "', error: " .. tostring(err))
    end
end

truncate_space('test_space')
truncate_space('orders')
