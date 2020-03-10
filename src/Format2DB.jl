module Format2DB

export main

using UUIDs, Dates, CSV, StructArrays, VideoIO, Tables, TableOperations, Dates
import Base.Threads: @spawn, @threads

include("resfile.jl")

function gettimes(path)
    times = CSV.File(joinpath(path, "calibration_times.csv")) |> Dict
    @assert all(file -> isfile(joinpath(path, file)), keys(times)) "video file/s missing"
    @assert all(file -> isfile(joinpath(path, string(first(splitext(file)), ".res"))), keys(times)) "res file/s missing"
    return times
end

function gettables(path, times, pixel)
    expname = basename(path)
    experiment = StructArray(((experiment = expname, experiment_description = "none", experiment_folder = path) for _ in 1:1))
    designation = Symbol(string("a", hash(path)))
    board = StructArray(((designation = designation, checker_width_cm = 3.9, checker_per_width = 2, checker_per_height = 2, board_description = "this is pretty bogus") for _ in 1:1))
    d = CSV.File(joinpath(path, "factors.csv")) |> Dict
    factors = (; Dict(Symbol(k) => v for (k, v) in d)...)
    x = (; Dict(k => String[] for k in keys(factors))...)
    run = StructArray((run = UUID[], experiment = String[], date = Date[], comment = String[], x...))
    video = StructArray((video = UUID[], comment = String[]))
    videofile = StructArray((file_name = String[], video = UUID[], date_time = DateTime[], duration = Nanosecond[], index = Int[]))
    calibration = StructArray((calibration = UUID[], intrinsic = Missing[], extrinsic = UUID[], board = Symbol[], comment = String[]))
    interval = StructArray((interval = UUID[], video = UUID[], start = Nanosecond[], stop = Union{Nanosecond, Missing}[], comment = String[]))
    poi = StructArray((poi = UUID[], type = Symbol[], run = UUID[], calibration = UUID[], interval = UUID[]))
    columns = CSV.File(joinpath(path, "columns.csv")) |> propertynames
    for (k, v) in times
        runid = uuid1()
        push!(run, (run = runid, experiment = expname, date = Date(now()), comment = k, factors...))
        videoid = uuid1()
        push!(video, (video = videoid, comment = k))
        date_time, _duration = VideoIO.get_time_duration(joinpath(path, k))
        duration = Nanosecond(round(Int, 1000000000_duration))
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
            push!(interval, (interval = id, video = videoid, start = Nanosecond(0), stop = column == :track ? Nanosecond(1000000000) : missing, comment = "bogus"))
            push!(poi, (poi = uuid1(), type = column, run = runid, calibration = calibrationid, interval = id))
        end
    end
    return filter(kv -> last(kv) isa StructArray, Base.@locals())
end

tonano(_::Missing) = missing
tonano(x) = Dates.value(Dates.Nanosecond(x))
saving(source, name, obj) = obj |> TableOperations.transform(start = tonano, stop = tonano, duration = tonano) |> CSV.write(joinpath(source, "$name.csv"))

function main(path; prefix = "source_")
    source = mktempdir(; prefix = prefix, cleanup = false)
    pixel = joinpath(source, "pixel")
    mkdir(pixel)
    # get the data
    times = gettimes(path)
    a = gettables(path, times, pixel)
    # save the data
    @sync for (k, v) in a
        @spawn saving(source, k, v)
    end
    @sync for file in keys(times)
        # @spawn symlink(joinpath(path, file), joinpath(source, file))
        @spawn cp(joinpath(path, file), joinpath(source, file))
    end
    return source
end

goodvideo(file) = first(file) ≠ '.' && occursin(r"mts|mp4|avi|mpg|mov|mkv"i, last(splitext(file)))

function joinsources(sources; prefix = "source_")
    source = mktempdir(; prefix = prefix, cleanup = false)
    pixel = joinpath(source, "pixel")
    mkdir(pixel)
    @sync for s in sources
        @spawn begin
            pp = joinpath(s, "pixel")
            for p in readdir(pp)
                @assert p ∉ readdir(pixel) "duplicate pixel files"
                # mv(joinpath(pp, p), joinpath(pixel, p))
                cp(joinpath(pp, p), joinpath(pixel, p))
            end
            # rm(pp)
        end
    end
    @sync for x in ("board", "calibration", "experiment", "interval", "poi", "run", "video", "videofile")
        @spawn begin 

            open(joinpath(source, "$x.csv"), "w") do i
                open(joinpath(sources[1], "$x.csv")) do o
                    write(i, read(o))
                end
                for s in Iterators.drop(sources, 1)
                    open(joinpath(s, "$x.csv")) do o
                        readuntil(o, '\n')
                        write(i, read(o))
                    end
                end
            end

            # for s in sources
                # rm(joinpath(s, "$x.csv"))
            # end
        end
    end
    @sync for s in sources
        @spawn begin
            for p in readdir(s)
                if goodvideo(p)
                    @assert p ∉ readdir(source) "duplicate video files"
                    cp(joinpath(s, p), joinpath(source, p))
                end
            end
        end
    end
    # for s in sources
    #     @assert isempty(readdir(s)) "some files remain?"
    #     rm(s)
    # end
    return source
end



end # module
