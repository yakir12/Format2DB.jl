module Format2DB

export main

using UUIDs, Dates, CSV, ProgressMeter, StructArrays, VideoIO, Tables

include("resfile.jl")

function gettimes(path)
    times = CSV.File(joinpath(path, "calibration_times.csv")) |> Dict
    @assert all(file -> isfile(joinpath(path, file)), keys(times)) "video file/s missing"
    @assert all(file -> isfile(joinpath(path, string(first(splitext(file)), ".res"))), keys(times)) "res file/s missing"
    return times
end

function gettables(path, times, pixel)
    expname = basename(path)
    experiment = StructArray(((experiment = expname, experiment_description = "", experiment_folder = "") for _ in 1:1))
    designation = :Temp
    board = StructArray(((designation = designation, checker_width_cm = 3.9, checker_per_width = 2, checker_per_height = 2, board_description = "this is pretty bogus") for _ in 1:1))
    d = CSV.File(joinpath(path, "factors.csv")) |> Dict
    factors = (; Dict(Symbol(k) => v for (k, v) in d)...)
    x = (; Dict(k => String[] for k in keys(factors))...)
    run = StructArray((run = UUID[], experiment = String[], date = Date[], comment = String[], x...))
    video = StructArray((video = UUID[], comment = String[]))
    videofile = StructArray((file_name = String[], video = UUID[], date_time = DateTime[], duration = Millisecond[], index = Int[]))
    calibration = StructArray((calibration = UUID[], intrinsic = Missing[], extrinsic = UUID[], board = Symbol[], comment = String[]))
    interval = StructArray((interval = UUID[], video = UUID[], start = Millisecond[], stop = Missing[], comment = String[]))
    poi = StructArray((poi = UUID[], type = Symbol[], run = UUID[], calibration = UUID[], interval = UUID[]))
    columns = CSV.File(joinpath(path, "columns.csv")) |> propertynames
    for (k, v) in times
        runid = uuid1()
        push!(run, (run = runid, experiment = expname, date = Date(now()), comment = k, factors...))
        videoid = uuid1()
        push!(video, (video = videoid, comment = k))
        date_time, _duration = VideoIO.get_time_duration(joinpath(path, k))
        duration = Millisecond(round(Int, 1000_duration))
        push!(videofile, (file_name = k, video = videoid, date_time = date_time, duration = duration, index = 1))
        calibrationid = uuid1()
        extrinsicid = uuid1()
        push!(calibration, (calibration = calibrationid, intrinsic = missing, extrinsic = extrinsicid, board = designation, comment = ""))
        intervalid = extrinsicid
        push!(interval, (interval = intervalid, video = videoid, start = v - Time(0), stop = missing, comment = ""))
        push!(poi, (poi = uuid1(), type = :calibration, run = runid, calibration = calibrationid, interval = intervalid))
        resfile = joinpath(path, string(first(splitext(k)), ".res"))
        ids = savepixels(pixel, resfile)
        for (column, id) in zip(columns, ids)
            push!(interval, (interval = id, video = videoid, start = Millisecond(0), stop = missing, comment = "bogus"))
            push!(poi, (poi = uuid1(), type = column, run = runid, calibration = calibrationid, interval = id))
        end
    end
    return filter(kv -> last(kv) isa StructArray, Base.@locals())
end

saving(source, name, obj) = CSV.write(joinpath(source, "$name.csv"), obj)

function main(path; prefix = "source_")
    source = mktempdir(homedir(), prefix = prefix, cleanup = false)
    pixel = joinpath(source, "pixel")
    mkdir(pixel)
    # get the data
    times = gettimes(path)
    a = gettables(path, times, pixel)
    # save the data
    for (k, v) in a
        saving(source, k, v)
    end
    @showprogress 1 "copying over files..." for file in keys(times)
        cp(joinpath(path, file), joinpath(source, file))
    end
    return source
end



end # module

# prefix = "source_"
# path = "/home/yakir/coffeebeetlearticle/displacement/Dtowards closed nest/displace_direction#towards displace_location#feeder person#therese nest#closed"
# main(path)



# function dictate(nt)
#     h, b = split(nt, first(keys(nt)))
#     Dict(first(h) => b)
# end
# vectorize(nt) = NamedTuple{keys(nt)}(Vector{t}() for t in nt)
# tableize(nt) = table(nt, pkey = first(keys(nt)))
# function make_empty(source)
#     video = (video = UUID, comment = String)
#     videofile = (file_name = String, video = UUID, date_time = DateTime, duration = Millisecond, index = Int)
#     interval = (interval = UUID, video = UUID, start = Millisecond, stop = Millisecond, comment = String)
#     poi = (poi = UUID, type = Symbol, run = UUID, calibration = UUID, interval = UUID)
#     calibration = (calibration = UUID, intrinsic = UUID, extrinsic = UUID, board = Symbol, comment = String)
#     board = (designation = Symbol, checker_width_cm = Float64, checker_per_width = Int, checker_per_height = Int, board_description = String)
#     experiment = (experiment = String, experiment_description = String, experiment_folder = String)
#     run = (run = UUID, experiment = String, comment = String)
#     return Dict(k => tableize(vectorize(v)) for (k,v) in Base.@locals() if v isa NamedTuple)
# end
# function getruns(path)
#     ress = Set{String}()
#     vids = Set{String}()
#     for file in readdir(path)
#         if isfile(joinpath(path, file))
#             name, ext = splitext(file)
#             if first(name) ≢ '.'
#                 if lowercase.(ext[2:end]) ∈ ("mts", "mp4", "avi", "mpg", "mov")
#                     push!(vids, name)
#                 elseif ext ≡ ".res"
#                     push!(ress, name)
#                 end
#             end
#         end
#     end
#     @assert ress == vids "video and res files do not match"
#     return collect(ress)
# end



