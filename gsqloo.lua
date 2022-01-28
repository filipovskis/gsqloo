--[[
MIT License

Copyright (c) 2022 Aleksandrs Filipovskis

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
--]]

local success = pcall(require, "mysqloo")

if (not success) then
    error("The library requires MySQLOO")
    return
end

gsqloo = gsqloo or {}

local colorWhite = color_white
local colorSuccess = Color(0, 255, 0)
local colorError = Color(255, 0, 0)

local function prepareArguments(query, lastIsCallback, ...)
    local count = select("#", ...)

    if (count > 0) then
        local lastArgument, callback

        if (lastIsCallback) then
            lastArgument = select(count, ...)

            if (isfunction(lastArgument)) then
                callback = lastArgument
                count = count - 1
            end
        end

        for i = 1, count do
            local arg = select(i, ...)

            if (arg == nil) then
                query:setNull(i)
            elseif (isnumber(arg)) then
                query:setNumber(i, arg)
            elseif (isbool(arg)) then
                query:setBoolean(i, arg)
            else
                query:setString(i, tostring(arg))
            end
        end

        if (lastIsCallback and callback) then
            query.onSuccess = function(self, data)
                callback(data, self)
            end
        end
    end
end

-- ANCHOR Transaction

local TRANSACTION = {}
TRANSACTION.__index = TRANSACTION

function TRANSACTION:Query(str)
    local query = self.database.handler:query(str)

    self.handler:addQuery(query)

    return self
end

function TRANSACTION:Prepare(str, ...)
    local query = self.database.handler:prepare(str)

    prepareArguments(query, false, ...)

    self.handler:addQuery(query)

    return self
end

function TRANSACTION:Start(callback)
    local obj = self.handler

    if (callback) then
        obj.onSuccess = function(query, data)
            callback(data, query)
        end
    end

    obj.onError = function(query, errorText)
        self.database:Error(errorText)
    end

    obj.onAborted = obj.onError

    obj:start()
end

-- ANCHOR Database

local DATABASE = {}
DATABASE.__index = DATABASE
DATABASE.__tostring = function(self)
    return (self.schema .. "@" .. self.hostname)
end

AccessorFunc(DATABASE, "connected", "Connected")

function DATABASE:Log(text)
    MsgC(Color(200, 200, 0), "[MySQL] ", colorWhite, tostring(self), " -> ", text, "\n")
end

function DATABASE:Error(text)
    MsgC(Color(200, 200, 0), "[MySQL] ", colorError, "[ERROR] ", colorWhite, tostring(self), " -> ", text, "\n")
end

function DATABASE:Success(text)
    MsgC(Color(200, 200, 0), "[MySQL] ", colorSuccess, "[SUCCESS] ", colorWhite, tostring(self), " -> ", text, "\n")
end

function DATABASE:Query(str, callback)
    local obj = self.handler:query(str)

    if (callback) then
        obj.onSuccess = function(query, data)
            callback(data, query)
        end
    end

    obj.onError = function(query, errorText)
        self:Error(errorText)
    end

    obj.onAborted = obj.onError

    obj:start()

    return obj
end

function DATABASE:Prepare(str, ...)
    local obj = self.handler:prepare(str)

    obj.onError = function(query, errorText)
        self:Error(errorText)
    end

    prepareArguments(obj, true, ...)

    obj:start()
end

function DATABASE:Transaction()
    local handler = self.handler:createTransaction()

    local transaction = setmetatable({
        database = self,
        handler = handler
    }, TRANSACTION)

    return transaction
end

function DATABASE:GetHandler()
    return self.handler
end

function DATABASE:GetHostName()
    return self.hostname
end

DATABASE.GetHost = DATABASE.GetHostName

-- ANCHOR Functions

function gsqloo.Create(hostname, username, password, schema, port, socket)
    local id = util.CRC(hostname .. "_" .. schema)
    local db = setmetatable({
        hostname = hostname,
        username = username,
        schema = schema,
        port = port,
        id = id
    }, DATABASE)

    db.handler = mysqloo.connect(hostname, username, password, schema, port, socket)
    db.handler.onConnected = function(handler)
        handler:setCharacterSet("utf8mb4")

        db:Success("Successfuly connected")
        db:SetConnected(true)

        hook.Run("gsqloo.OnConnected", db)
    end
    db.handler.onConnectionFailed = function(handler, errorText)
        db:Error(errorText)
    end

    -- Keep database connection stable
    timer.Create("gsqloo.Ping_" .. id, 300, 0, function()
        db.handler:ping()
    end)

    db.handler:connect()

    return db
end