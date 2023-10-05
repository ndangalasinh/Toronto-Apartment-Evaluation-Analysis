{% macro get_ward_name(ward_number) %}

    case
        {{ ward_number }}
        when 1
        then 'Etobicoke North'
        when 2
        then 'Etobicoke Centre'
        when 3
        then 'Etobicoke—Lakeshore'
        when 4
        then 'Parkdale—High Park'
        when 5
        then 'York South—Weston'
        when 6
        then 'York Centre'
        when 7
        then 'Humber River—Black Creek'
        when 8
        then 'Eglinton—Lawrence'
        when 9
        then 'Davenport'
        when 10
        then 'Spadina—Fort York'
        when 11
        then 'University—Rosedale'
        when 12
        then 'Toronto—St. Pauls'
        when 13
        then 'Toronto Centre'
        when 14
        then 'Toronto—Danforth'
        when 15
        then 'Don Valley West'
        when 16
        then 'Don Valley East'
        when 17
        then 'Don Valley North'
        when 18
        then 'Willowdale'
        when 19
        then 'Beaches-East York'
        when 20
        then 'Scarborough Southwest'
        when 21
        then 'Scarborough Centre'
        when 22
        then 'Scarborough—Agincourt'
        when 23
        then 'Scarborough North'
        when 24
        then 'Scarborough—Guildwood'
        when 25
        then 'Scarborough—Rouge Park'
    end

{% endmacro %}
